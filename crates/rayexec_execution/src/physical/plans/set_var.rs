use crate::engine::modify::Modification;
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use super::{PollPull, SourceOperator2};

#[derive(Debug)]
pub struct PhysicalSetVar {
    var: String,
    value: OwnedScalarValue,
    sent: AtomicBool,
}

impl PhysicalSetVar {
    pub fn new(var: String, value: OwnedScalarValue) -> Self {
        PhysicalSetVar {
            var,
            value,
            sent: false.into(),
        }
    }

    fn set_inner(&self, task_cx: &TaskContext) -> Result<()> {
        let chan = match &task_cx.modifications {
            Some(chan) => chan,
            None => return Err(RayexecError::new("Session cannot be modified")),
        };

        chan.send(Modification::UpdateVariable {
            name: self.var.clone(),
            value: self.value.clone(),
        })
        .context("Failed to send on modifications channel")?;

        self.sent.store(true, Ordering::Relaxed);

        Ok(())
    }
}

impl SourceOperator2 for PhysicalSetVar {
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

        self.set_inner(task_cx)?;
        Ok(PollPull::Batch(Batch::empty()))
    }
}

impl Explainable for PhysicalSetVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalSetVar")
            .with_value("var", &self.var)
            .with_value("value", &self.value)
    }
}
