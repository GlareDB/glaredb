use std::task::Context;

use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;

#[derive(Debug)]
pub struct PhysicalProject {
    pub(crate) projections: Vec<PhysicalScalarExpression>,
}

#[derive(Debug)]
pub struct ProjectPartitionState {
    evaluator: ExpressionEvaluator,
}

impl ExecutableOperator for PhysicalProject {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        let partition_states = (0..partitions)
            .map(|_| {
                PartitionState::Project(ProjectPartitionState {
                    evaluator: ExpressionEvaluator::new(self.projections.clone(), batch_size),
                })
            })
            .collect();

        Ok(PartitionAndOperatorStates::Branchless {
            operator_state: OperatorState::None,
            partition_states,
        })
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Project(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        let input = inout.input.required("batch input")?;
        let output = inout.output.required("batch output")?;

        // TODO: Reset output.

        let sel = input.generate_selection();
        state.evaluator.eval_batch(input, sel, output)?;

        Ok(PollExecute::Ready)
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalProject {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}
