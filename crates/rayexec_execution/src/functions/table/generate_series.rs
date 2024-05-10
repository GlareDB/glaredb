use super::{BoundTableFunctionOld, Pushdown, Statistics, TableFunctionArgs, TableFunctionOld};
use crate::physical::plans::{PollPull, SourceOperator2};
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::array::{Array, Int32Array};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::{DataType, Field, Schema};
use rayexec_error::{RayexecError, Result};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy)]
pub struct GenerateSeries;

impl TableFunctionOld for GenerateSeries {
    fn name(&self) -> &str {
        "generate_series"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunctionOld>> {
        if !args.named.is_empty() {
            return Err(RayexecError::new(
                "This function doesn't accept named arguments".to_string(),
            ));
        }

        let (start, stop, step) = match args.unnamed.len() {
            2 => (
                args.unnamed.first().unwrap().try_as_i32()?,
                args.unnamed.get(1).unwrap().try_as_i32()?,
                1,
            ),
            3 => (
                args.unnamed.first().unwrap().try_as_i32()?,
                args.unnamed.get(1).unwrap().try_as_i32()?,
                args.unnamed.get(2).unwrap().try_as_i32()?,
            ),
            other => {
                return Err(RayexecError::new(format!(
                    "Expected 2 or 3 arguments, got {other}"
                )))
            }
        };

        Ok(Box::new(GenerateSeriesInteger {
            start,
            stop,
            step,
            schema: Schema::new([Field::new("generate_series", DataType::Int32, false)]),
        }))
    }
}

#[derive(Debug, Clone)]
struct GenerateSeriesInteger {
    start: i32,
    stop: i32,
    step: i32,
    schema: Schema,
}

impl BoundTableFunctionOld for GenerateSeriesInteger {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            estimated_cardinality: None,
            max_cardinality: None,
        }
    }

    fn into_source(
        self: Box<Self>,
        _projection: Vec<usize>,
        _pushdown: Pushdown,
    ) -> Result<Box<dyn SourceOperator2>> {
        Ok(Box::new(GenerateSeriesIntegerOperator {
            start: self.start,
            stop: self.stop,
            step: self.step,
            curr: AtomicI32::new(self.start),
        }))
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
    start: i32,
    stop: i32,
    step: i32,
    curr: AtomicI32,
}

impl SourceOperator2 for GenerateSeriesIntegerOperator {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_pull(
        &self,
        _task_cx: &TaskContext,
        _cx: &mut Context<'_>,
        _partition: usize,
    ) -> Result<PollPull> {
        const BATCH_SIZE: usize = 1000;
        let curr = self.curr.load(Ordering::Relaxed);

        if curr > self.stop {
            return Ok(PollPull::Exhausted);
        }

        let vals: Vec<_> = (curr..=self.stop)
            .step_by(self.step as usize)
            .take(BATCH_SIZE)
            .collect();

        let last = match vals.last() {
            Some(last) => *last,
            None => return Ok(PollPull::Exhausted),
        };

        self.curr.store(last + self.step, Ordering::Relaxed);
        let arr = Array::Int32(Int32Array::from(vals));

        Ok(PollPull::Batch(Batch::try_new([arr]).unwrap()))
    }
}

impl Explainable for GenerateSeriesIntegerOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let ent = ExplainEntry::new("generate_series (int)");
        if conf.verbose {
            ent.with_value("start", self.start)
                .with_value("stop", self.stop)
                .with_value("step", self.step)
        } else {
            ent
        }
    }
}
