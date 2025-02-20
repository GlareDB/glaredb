use std::task::Context;

use rayexec_error::{OptionExt, RayexecError, Result};

use super::operation::{self, PartitionSource, Projections};
use crate::database::DatabaseContext;
use crate::execution::operators::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::functions::table::{PlannedTableFunction, TableFunctionImpl};

#[derive(Debug)]
pub struct TableFunctionPartitionState {
    source: Box<dyn PartitionSource>,
}

#[derive(Debug)]
pub struct PhysicalTableFunction {
    pub(crate) projections: Projections,
    pub(crate) table_function: PlannedTableFunction,
}

impl ExecutableOperator for PhysicalTableFunction {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Self::States> {
        let mut scan = match &self.table_function.function_impl {
            TableFunctionImpl::Scan(scan) => scan.lock(),
            _ => {
                return Err(RayexecError::new(format!(
                    "Table function '{}' not a scan function",
                    self.table_function.function.name()
                )))
            }
        };

        let sources =
            scan.create_partition_sources(context, &self.projections, batch_size, partitions)?;
        let partition_states = sources
            .into_iter()
            .map(|source| PartitionState::TableFunction(TableFunctionPartitionState { source }))
            .collect();

        Ok(UnaryInputStates {
            operator_state: OperatorState::None,
            partition_states,
        })
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::TableFunction(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let output = inout.output.required("output batch required")?;
        output.reset_for_write()?;

        match state.source.poll_pull(cx, output)? {
            operation::PollPull::HasMore => Ok(PollExecute::HasMore),
            operation::PollPull::Pending => Ok(PollExecute::Pending),
            operation::PollPull::Exhausted => Ok(PollExecute::Exhausted),
        }
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

impl Explainable for PhysicalTableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableFunction")
    }
}
