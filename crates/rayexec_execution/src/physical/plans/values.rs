use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Poll};

use super::{PollPull, SourceOperator2};

#[derive(Debug)]
pub struct PhysicalValues {
    batch: Mutex<Option<Batch>>,
}

impl PhysicalValues {
    pub fn new(batch: Batch) -> Self {
        PhysicalValues {
            batch: Mutex::new(Some(batch)),
        }
    }
}

impl SourceOperator2 for PhysicalValues {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_pull(
        &self,
        _task_cx: &TaskContext,
        _cx: &mut Context,
        _partition: usize,
    ) -> Result<PollPull> {
        match self.batch.lock().take() {
            Some(batch) => Ok(PollPull::Batch(batch)),
            None => Ok(PollPull::Exhausted),
        }
    }
}

impl Explainable for PhysicalValues {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Values")
    }
}
