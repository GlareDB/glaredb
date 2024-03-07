use super::{BoundTableFunction, Pushdown, Statistics, TableFunction, TableFunctionArgs};
use crate::{
    physical::{plans::Source, TaskContext},
    planner::explainable::{ExplainConfig, ExplainEntry, Explainable},
    types::batch::{DataBatch, NamedDataBatchSchema},
};
use arrow_array::StringArray;
use arrow_schema::DataType;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DummyTableFunction;

impl TableFunction for DummyTableFunction {
    fn name(&self) -> &str {
        "dummy"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunction>> {
        if !args.unnamed.is_empty() || !args.named.is_empty() {
            return Err(RayexecError::new(
                "Dummy table functions accepts no arguments",
            ));
        }
        Ok(Box::new(BoundDummyTableFunction))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundDummyTableFunction;

impl BoundTableFunction for BoundDummyTableFunction {
    fn schema(&self) -> NamedDataBatchSchema {
        NamedDataBatchSchema::try_new(vec!["dummy".to_string()], vec![DataType::Utf8]).unwrap()
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            estimated_cardinality: Some(1),
            max_cardinality: Some(1),
        }
    }

    fn into_source(
        self: Box<Self>,
        projection: Vec<usize>,
        _pushdown: Pushdown,
    ) -> Result<Box<dyn Source>> {
        Ok(Box::new(DummyTableFunctionSource::new(projection)))
    }
}

impl Explainable for BoundDummyTableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Dummy")
    }
}

#[derive(Debug)]
pub struct DummyTableFunctionSource {
    projection: Vec<usize>,
    batch: Mutex<Option<DataBatch>>,
}

impl DummyTableFunctionSource {
    fn new(projection: Vec<usize>) -> Self {
        let batch = DataBatch::try_new(vec![Arc::new(StringArray::from(vec!["dummy"]))]).unwrap();
        DummyTableFunctionSource {
            projection,
            batch: Mutex::new(Some(batch)),
        }
    }
}

impl Source for DummyTableFunctionSource {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_next(
        &self,
        _task_cx: &TaskContext,
        _cx: &mut Context<'_>,
        _partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        match self.batch.lock().take() {
            Some(batch) => Poll::Ready(Some(Ok(batch.project(&self.projection)))),
            None => Poll::Ready(None),
        }
    }
}

impl Explainable for DummyTableFunctionSource {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Dummy").with_values("projection", self.projection.clone())
    }
}
