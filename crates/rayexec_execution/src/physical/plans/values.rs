use crate::expr::Expression;
use crate::hash::build_hashes;
use crate::physical::PhysicalOperator2;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, DataBatchSchema};
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{Field, Schema};
use hashbrown::raw::RawTable;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use smallvec::{smallvec, SmallVec};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{buffer::BatchBuffer, Sink2, Source2};

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

impl Source2 for PhysicalValues {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_partition(
        &self,
        _cx: &mut Context<'_>,
        _partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        match self.batch.lock().take() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => Poll::Ready(None),
        }
    }
}

impl Sink2 for PhysicalValues {
    fn push(&self, _input: DataBatch, _child: usize, _partition: usize) -> Result<()> {
        Err(RayexecError::new("Cannot push to values"))
    }

    fn finish(&self, _child: usize, _partition: usize) -> Result<()> {
        Err(RayexecError::new("Cannot finish values"))
    }
}

impl PhysicalOperator2 for PhysicalValues {}

impl Explainable for PhysicalValues {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Values")
    }
}
