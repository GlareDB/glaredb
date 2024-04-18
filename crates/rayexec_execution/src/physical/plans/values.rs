use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;

use parking_lot::Mutex;
use rayexec_error::Result;
use std::task::{Context, Poll};

use super::{PollPull, Source};

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
