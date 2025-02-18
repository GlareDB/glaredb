use std::task::Context;

use rayexec_error::Result;

use super::{ExecutableOperator, OperatorState, PartitionState, PollFinalize, UnaryInputStates};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Physical operator for EXPLAIN ANALYZE.
#[derive(Debug)]
pub struct PhysicalAnalyze {}

impl ExecutableOperator for PhysicalAnalyze {
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

impl Explainable for PhysicalAnalyze {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Analyze")
    }
}
