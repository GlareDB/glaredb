use std::sync::Arc;
use std::task::Context;

use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::functions::table::{inout, PlannedTableFunction, TableFunctionImpl};

#[derive(Debug)]
pub struct TableInOutPartitionState {
    function_state: Box<dyn inout::TableInOutPartitionState>,
}

#[derive(Debug)]
pub struct PhysicalTableInOut {
    /// The table function.
    pub function: PlannedTableFunction,
    /// Input expressions to the table function.
    pub function_inputs: Vec<PhysicalScalarExpression>,
}

impl ExecutableOperator for PhysicalTableInOut {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        let states = match &self.function.function_impl {
            TableFunctionImpl::InOut(function) => function.create_states(partitions)?,
            _ => {
                return Err(RayexecError::new(format!(
                    "'{}' is not a table in/out function",
                    self.function.function.name()
                )))
            }
        };

        let states: Vec<_> = states
            .into_iter()
            .map(|state| {
                PartitionState::TableInOut(TableInOutPartitionState {
                    function_state: state,
                })
            })
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states,
            },
        })
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::TableInOut(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.function_state.poll_push(cx, batch)
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::TableInOut(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        state.function_state.poll_finalize_push(cx)
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::TableInOut(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.function_state.poll_pull(cx)
    }
}

impl Explainable for PhysicalTableInOut {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableInOut")
    }
}
