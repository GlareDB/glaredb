pub mod encode;
pub mod partition_state;

mod merge;
mod sort_data;

use std::task::Context;

use rayexec_error::{OptionExt, Result};

use super::{ExecutableOperator, ExecuteInOutState, OperatorState, PartitionState, PollExecute, PollFinalize};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::{PhysicalScalarExpression, PhysicalSortExpression};

#[derive(Debug)]
pub struct PhysicalSort {
    pub(crate) exprs: Vec<PhysicalSortExpression>,
}

impl ExecutableOperator for PhysicalSort {
    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        unimplemented!()
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalSort {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}
