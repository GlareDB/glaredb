use crate::engine::modify::Modification;
use crate::engine::vars::SessionVar;
use crate::expr::scalar::ScalarValue;
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use super::{PollPull, Source};

#[derive(Debug)]
pub struct PhysicalShowVar {
    var: SessionVar,
    sent: AtomicBool,
}

impl PhysicalShowVar {
    pub fn new(var: SessionVar) -> Self {
        PhysicalShowVar {
            var,
            sent: false.into(),
        }
    }
}

impl Source for PhysicalShowVar {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_pull(
        &self,
        task_cx: &TaskContext,
        _cx: &mut Context,
        partition: usize,
    ) -> Result<PollPull> {
        assert_eq!(0, partition);
        if self.sent.load(Ordering::Relaxed) {
            return Ok(PollPull::Exhausted);
        }

        let arr = self
            .var
            .value
            .as_array(1)
            .expect("session variable to convert to array without error");
        let batch =
            DataBatch::try_new(vec![arr]).expect("creating a batch for a session var to not error");

        self.sent.store(true, Ordering::Relaxed);

        Ok(PollPull::Batch(batch))
    }
}

impl Explainable for PhysicalShowVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalShowVar").with_value("var", &self.var.name)
    }
}
