use std::task::{Context, Waker};

use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::executor::physical_type::PhysicalList;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use super::{
    ExecutableOperator,
    ExecutionStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;

#[derive(Debug)]
pub struct UnnestPartitionState {
    /// Inputs we're processing.
    inputs: Vec<Array>,
    input_num_rows: usize,
    current_row: usize,
    push_waker: Option<Waker>,
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalUnnest {
    pub expressions: Vec<PhysicalScalarExpression>,
}

impl ExecutableOperator for PhysicalUnnest {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        unimplemented!()
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::Unnest(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if state.current_row < state.input_num_rows {
            // Still processing inputs, come back later.
            state.push_waker = Some(cx.waker().clone());
            if let Some(waker) = state.pull_waker.take() {
                waker.wake();
            }

            return Ok(PollPush::Pending(batch));
        }

        // Compute inputs. These will be stored until we've processed all rows.
        for (col_idx, expr) in self.expressions.iter().enumerate() {
            state.inputs[col_idx] = expr.eval(&batch)?.into_owned();
        }

        unimplemented!()
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::Unnest(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if state.current_row >= state.input_num_rows {
            // We're done with these inputs. Come back later.
            state.pull_waker = Some(cx.waker().clone());
            if let Some(waker) = state.push_waker.take() {
                waker.wake();
            }

            return Ok(PollPull::Pending);
        }

        // We have input ready, get the longest list for the current row.
        let mut longest = 0;
        for input_idx in 0..state.inputs.len() {
            if let Some(list_meta) = UnaryExecutor::value_at::<PhysicalList>(
                &state.inputs[input_idx],
                state.current_row,
            )? {
                if list_meta.len > longest {
                    longest = list_meta.len;
                }
            }
        }

        unimplemented!()
    }
}

impl Explainable for PhysicalUnnest {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Unnest")
    }
}
