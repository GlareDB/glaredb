use crate::expr::Expression;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, DataBatchSchema};
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{Field, Schema};
use hashbrown::raw::RawTable;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::task::{Context, Poll};

use super::Source;

#[derive(Debug)]
pub struct PhysicalValues {
    batch: Mutex<Option<DataBatch>>,
}

impl PhysicalValues {
    pub fn new(batch: DataBatch) -> Self {
        PhysicalValues {
            batch: Mutex::new(Some(batch)),
        }
    }
}

impl Source for PhysicalValues {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_next(&self, _cx: &mut Context, _partition: usize) -> Poll<Option<Result<DataBatch>>> {
        match self.batch.lock().take() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => Poll::Ready(None),
        }
    }
}

impl Explainable for PhysicalValues {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Values")
    }
}
