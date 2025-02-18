use std::task::Context;

use rayexec_error::Result;

use super::{ExecutableOperator, OperatorState, PartitionState, PollFinalize, UnaryInputStates};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct PhysicalWindow {}

impl ExecutableOperator for PhysicalWindow {
    type States = UnaryInputStates;

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalWindow {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Window")
    }
}
