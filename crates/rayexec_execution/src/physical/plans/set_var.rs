use crate::engine::modify::Modification;
use crate::expr::scalar::ScalarValue;
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use super::Source;

#[derive(Debug)]
pub struct PhysicalSetVar {
    var: String,
    value: ScalarValue,
    sent: AtomicBool,
}

impl PhysicalSetVar {
    pub fn new(var: String, value: ScalarValue) -> Self {
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

impl Source for PhysicalSetVar {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_next(
        &self,
        task_cx: &TaskContext,
        _cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        assert_eq!(0, partition);
        if self.sent.load(Ordering::Relaxed) {
            return Poll::Ready(None);
        }

        match self.set_inner(task_cx) {
            Ok(_) => Poll::Ready(Some(Ok(DataBatch::empty()))),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl Explainable for PhysicalSetVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalSetVar")
            .with_value("var", &self.var)
            .with_value("value", &self.value)
    }
}
