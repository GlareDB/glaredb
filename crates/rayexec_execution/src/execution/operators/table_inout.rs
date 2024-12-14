use std::sync::Arc;
use std::task::{Context, Waker};

use rayexec_bullet::array::Array;
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
    project_inputs: Vec<Array>,
    function_inputs: Vec<Array>,
    function_states: Vec<Box<dyn inout::TableInOutPartitionState>>,
    /// Number of rows in the input.
    input_num_rows: usize,
    /// Row we're currently unnesting.
    current_row: usize,
    /// If inputs are finished.
    finished: bool,
    /// Push side waker.
    ///
    /// Set if we still have rows to process.
    push_waker: Option<Waker>,
    /// Pull side waker.
    ///
    /// Set if we've processed all rows and need more input.
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalTableInOut {
    /// The table functions.
    ///
    /// Output lengths from each function may differ. If lengths differ, arrays
    /// will be extended with NULL values until all arrays have the same length.
    pub functions: Vec<PlannedTableFunction>,
    /// Expressions used to compute the inputs to the functions.
    ///
    /// Holds expressions for all functions.
    pub function_expressions: Vec<PhysicalScalarExpression>,
    /// (offset, len) pairs for each function to jump to the right expressions
    /// for a given function.
    pub function_offsets: Vec<(usize, usize)>,
    /// Expressions that will be projected out of the operator.
    ///
    /// The output of the expressions will be cross joined with the output of
    /// the functions. Cross joining happens after NULL extension.
    pub project_expressions: Vec<PhysicalScalarExpression>,
}

impl ExecutableOperator for PhysicalTableInOut {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        unimplemented!()
        // let states = match &self.function.function_impl {
        //     TableFunctionImpl::InOut(function) => function.create_states(partitions)?,
        //     _ => {
        //         return Err(RayexecError::new(format!(
        //             "'{}' is not a table in/out function",
        //             self.function.function.name()
        //         )))
        //     }
        // };

        // let states: Vec<_> = states
        //     .into_iter()
        //     .map(|state| PartitionState::TableInOut(TableInOutPartitionState { state }))
        //     .collect();

        // Ok(ExecutionStates {
        //     operator_state: Arc::new(OperatorState::None),
        //     partition_states: InputOutputStates::OneToOne {
        //         partition_states: states,
        //     },
        // })
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

        if state.current_row < state.input_num_rows {
            // Still processing inputs, come back later.
            state.push_waker = Some(cx.waker().clone());
            if let Some(waker) = state.pull_waker.take() {
                waker.wake();
            }

            return Ok(PollPush::Pending(batch));
        }

        // Compute inputs. These will be stored until we've processed all rows.
        for (col_idx, expr) in self.project_expressions.iter().enumerate() {
            state.project_inputs[col_idx] = expr.eval(&batch)?.into_owned();
        }

        for (col_idx, expr) in self.function_expressions.iter().enumerate() {
            state.function_inputs[col_idx] = expr.eval(&batch)?.into_owned();
        }

        state.input_num_rows = batch.num_rows();
        state.current_row = 0;

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollPush::Pushed)
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

        state.finished = true;

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalize::Finalized)
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

        if state.current_row >= state.input_num_rows {
            if state.finished {
                return Ok(PollPull::Exhausted);
            }

            // We're done with these inputs. Come back later.
            state.pull_waker = Some(cx.waker().clone());
            if let Some(waker) = state.push_waker.take() {
                waker.wake();
            }

            return Ok(PollPull::Pending);
        }

        let mut outputs =
            Vec::with_capacity(state.function_inputs.len() + state.project_inputs.len());

        // state.state.poll_pull(cx)
        unimplemented!()
    }
}

impl Explainable for PhysicalTableInOut {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableInOut")
    }
}
