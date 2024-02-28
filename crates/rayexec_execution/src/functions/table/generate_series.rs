use super::{BoundTableFunction, Pushdown, Statistics, TableFunction, TableFunctionArgs};
use crate::expr::scalar::ScalarValue;
use crate::physical::plans::{Sink, Source};
use crate::physical::PhysicalOperator;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, NamedDataBatchSchema};
use arrow_array::Int32Array;
use arrow_schema::DataType;
use rayexec_error::{RayexecError, Result};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy)]
pub struct GenerateSeries;

impl TableFunction for GenerateSeries {
    fn name(&self) -> &str {
        "generate_series"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunction>> {
        fn get_i32(scalar: &ScalarValue) -> Result<i32> {
            Ok(match scalar {
                ScalarValue::Int32(i) => *i,
                ScalarValue::Int64(i) => *i as i32, // TODO
                other => {
                    return Err(RayexecError::new(format!(
                        "Expected integer argument, got {other:?}"
                    )))
                }
            })
        }

        if !args.named.is_empty() {
            return Err(RayexecError::new(format!(
                "This function doesn't accept named arguments"
            )));
        }

        let (start, stop, step) = match args.unnamed.len() {
            2 => (
                get_i32(args.unnamed.get(0).unwrap())?,
                get_i32(args.unnamed.get(1).unwrap())?,
                1,
            ),
            3 => (
                get_i32(args.unnamed.get(0).unwrap())?,
                get_i32(args.unnamed.get(1).unwrap())?,
                get_i32(args.unnamed.get(2).unwrap())?,
            ),
            other => {
                return Err(RayexecError::new(format!(
                    "Expected 2 or 3 arguments, got {other}"
                )))
            }
        };

        Ok(Box::new(GenerateSeriesInteger { start, stop, step }))
    }
}

#[derive(Debug, Clone, Copy)]
struct GenerateSeriesInteger {
    start: i32,
    stop: i32,
    step: i32,
}

impl BoundTableFunction for GenerateSeriesInteger {
    fn schema(&self) -> NamedDataBatchSchema {
        NamedDataBatchSchema::try_new(vec!["generate_series"], vec![DataType::Int32]).unwrap()
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            estimated_cardinality: None,
            max_cardinality: None,
        }
    }

    fn create_operator(
        &self,
        projection: Vec<usize>,
        pushdown: Pushdown,
    ) -> Arc<dyn PhysicalOperator> {
        Arc::new(GenerateSeriesIntegerOperator {
            s: *self,
            curr: AtomicI32::new(self.start),
        })
    }
}

impl Explainable for GenerateSeriesInteger {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let ent = ExplainEntry::new(GenerateSeries.name());
        if conf.verbose {
            ent.with_value("start", self.start)
                .with_value("stop", self.stop)
                .with_value("step", self.step)
        } else {
            ent
        }
    }
}

#[derive(Debug)]
struct GenerateSeriesIntegerOperator {
    s: GenerateSeriesInteger,
    curr: AtomicI32,
}

impl Source for GenerateSeriesIntegerOperator {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        const BATCH_SIZE: usize = 1000;
        let mut curr = self.curr.load(Ordering::Relaxed);

        if curr > self.s.stop {
            return Poll::Ready(None);
        }

        let vals: Vec<_> = (curr..=self.s.stop)
            .step_by(self.s.step as usize)
            .take(BATCH_SIZE)
            .collect();

        let last = match vals.last() {
            Some(last) => *last,
            None => return Poll::Ready(None),
        };

        self.curr.store(last + self.s.step, Ordering::Relaxed);
        let arr = Arc::new(Int32Array::from(vals));

        Poll::Ready(Some(Ok(DataBatch::try_new(vec![arr]).unwrap())))
    }
}

impl Sink for GenerateSeriesIntegerOperator {
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
        Err(RayexecError::new("Cannot push to generate series"))
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        Err(RayexecError::new("Cannot finish generate series"))
    }
}

impl Explainable for GenerateSeriesIntegerOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.s.explain_entry(conf)
    }
}

impl PhysicalOperator for GenerateSeriesIntegerOperator {}
