use std::sync::Arc;
use std::task::Context;

use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::selection::SelectionVector;
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
    /// Additional outputs that will be included on the output batch.
    additional_outputs: Vec<ArrayOld>,
}

#[derive(Debug)]
pub struct PhysicalTableInOut {
    /// The table function.
    pub function: PlannedTableFunction,
    /// Input expressions to the table function.
    pub function_inputs: Vec<PhysicalScalarExpression>,
    /// Output projections.
    pub projected_outputs: Vec<PhysicalScalarExpression>,
}

impl ExecutableOperator for PhysicalTableInOut {
    fn create_states_old(
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
                    additional_outputs: Vec::new(),
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

    fn poll_push_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: BatchOld,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::TableInOut(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        // TODO: Don't do this.
        let orig = batch.clone();

        let inputs = self
            .function_inputs
            .iter()
            .map(|expr| {
                let arr = expr.eval2(&batch)?;
                Ok(arr.into_owned())
            })
            .collect::<Result<Vec<_>>>()?;

        let inputs = BatchOld::try_new(inputs)?;

        // Try to push first to avoid overwriting any buffered additional
        // outputs.
        //
        // If we get a Pending, we need to return early with the original batch.
        //
        // TODO: Remove needing to do this, the clones should be cheap, but the
        // expression execution is wasteful.
        match state.function_state.poll_push(cx, inputs)? {
            PollPush::Pending(_) => Ok(PollPush::Pending(orig)),
            other => {
                // Batch was pushed to the function state, compute additional
                // outputs.
                let additional_outputs = self
                    .projected_outputs
                    .iter()
                    .map(|expr| {
                        let arr = expr.eval2(&batch)?;
                        Ok(arr.into_owned())
                    })
                    .collect::<Result<Vec<_>>>()?;

                state.additional_outputs = additional_outputs;

                Ok(other)
            }
        }
    }

    fn poll_finalize_push_old(
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

    fn poll_pull_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::TableInOut(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.function_state.poll_pull(cx)? {
            inout::InOutPollPull::Batch { batch, row_nums } => {
                // We got a batch, append additional outputs according to
                // returned row numbers.
                if batch.num_rows() != row_nums.len() {
                    return Err(RayexecError::new("Row number mismatch").with_fields([
                        ("batch_num_rows", batch.num_rows()),
                        ("row_nums_len", row_nums.len()),
                    ]));
                }

                let selection = Arc::new(SelectionVector::from(row_nums));

                let mut arrays = batch.into_arrays();
                arrays.reserve(state.additional_outputs.len());

                for additional in &state.additional_outputs {
                    let mut additional = additional.clone();
                    additional.select_mut(selection.clone());
                    arrays.push(additional);
                }

                let new_batch = BatchOld::try_new(arrays)?;

                Ok(PollPull::Computed(new_batch.into()))
            }
            inout::InOutPollPull::Pending => Ok(PollPull::Pending),
            inout::InOutPollPull::Exhausted => Ok(PollPull::Exhausted),
        }
    }
}

impl Explainable for PhysicalTableInOut {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableInOut")
    }
}
