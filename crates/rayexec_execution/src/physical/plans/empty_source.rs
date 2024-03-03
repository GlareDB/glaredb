use super::Source;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;
use rayexec_error::{RayexecError, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

/// Produces an empty batch with one logical row.
///
/// This serves to drive execution for parts of the plan that have no physical
/// scans of tables or functions (e.g. SELECT 1);
#[derive(Debug)]
pub struct EmptySource {
    finished: AtomicBool,
}

impl EmptySource {
    pub fn new() -> Self {
        EmptySource {
            finished: AtomicBool::new(false),
        }
    }
}

impl Source for EmptySource {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_next(
        &self,
        _cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        assert_eq!(0, partition);
        match self
            .finished
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => Poll::Ready(Some(Ok(DataBatch::empty_with_num_rows(1)))),
            Err(_) => Poll::Ready(None),
        }
    }
}

impl Explainable for EmptySource {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("EmptySource")
    }
}
