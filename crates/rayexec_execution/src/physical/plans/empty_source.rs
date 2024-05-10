use super::{PollPull, SourceOperator2};
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Context;

/// Produces an empty batch with one logical row.
///
/// This serves to drive execution for parts of the plan that have no physical
/// scans of tables or functions (e.g. SELECT 1);
#[derive(Debug)]
pub struct EmptySource {
    finished: AtomicBool,
}

impl Default for EmptySource {
    fn default() -> Self {
        Self::new()
    }
}

impl EmptySource {
    pub fn new() -> Self {
        EmptySource {
            finished: AtomicBool::new(false),
        }
    }
}

impl SourceOperator2 for EmptySource {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_pull(
        &self,
        _task_cx: &TaskContext,
        _cx: &mut Context<'_>,
        partition: usize,
    ) -> Result<PollPull> {
        assert_eq!(0, partition);
        match self
            .finished
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => Ok(PollPull::Batch(Batch::empty_with_num_rows(1))),
            Err(_) => Ok(PollPull::Exhausted),
        }
    }
}

impl Explainable for EmptySource {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("EmptySource")
    }
}
