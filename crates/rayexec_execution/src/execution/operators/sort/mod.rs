mod merge_queue;

use std::task::Context;

use merge_queue::{MergeQueue, PollMerge};
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::sort::partial_sort::{PartialSortedRowCollection, SortedRowAppendState};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::PhysicalSortExpression;

#[derive(Debug)]
pub enum SortPartitionState {
    /// Partition is collecting data for sorting.
    Collecting(SortCollectingState),
    /// Partition is currently working on merging sorted runs in the queue.
    Merging(SortMergingState),
}

#[derive(Debug)]
pub struct SortCollectingState {
    partition_idx: usize,
    /// Evaluator sort expressions to produce sort keys.
    evaluator: ExpressionEvaluator,
    /// Evaluator output.
    keys: Batch,
    /// State for appending new batches to sort collection.
    append: SortedRowAppendState,
    /// Hold partially sorted data for this partition.
    collection: PartialSortedRowCollection,
}

#[derive(Debug)]
pub struct SortMergingState {
    partition_idx: usize,
}

#[derive(Debug)]
pub struct SortOperatorState {
    queue: MergeQueue,
}

#[derive(Debug)]
pub struct PhysicalSort {
    pub(crate) exprs: Vec<PhysicalSortExpression>,
    pub(crate) output_types: Vec<DataType>,
}

impl ExecutableOperator for PhysicalSort {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Self::States> {
        unimplemented!()
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Sort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::Sort(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            SortPartitionState::Collecting(state) => {
                let input = inout.input.required("input batch required")?;

                state.keys.reset_for_write()?;
                state
                    .evaluator
                    .eval_batch(input, input.selection(), &mut state.keys)?;

                let num_rows = input.num_rows();
                debug_assert_eq!(num_rows, state.keys.num_rows());

                state.collection.append_unsorted_keys_and_data(
                    &mut state.append,
                    &state.keys.arrays,
                    &input.arrays,
                    num_rows,
                )?;

                Ok(PollExecute::NeedsMore)
            }
            SortPartitionState::Merging(state) => {
                // Make progress in merging.
                let poll = operator_state
                    .queue
                    .poll_merge_next(cx, state.partition_idx)?;
                match poll {
                    PollMerge::Merged => {
                        // TODO: Unsure if we just want to loop here. By waking
                        // our own waker, we yield back to the executor for
                        // fairness.
                        cx.waker().wake_by_ref();
                        Ok(PollExecute::Pending)
                    }
                    PollMerge::Pending => {
                        // We'll be woken up when there's more to merge.
                        Ok(PollExecute::Pending)
                    }
                    PollMerge::Finished => {
                        // TODO
                        unimplemented!()
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::Sort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::Sort(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            SortPartitionState::Collecting(collect_state) => {
                let partition_idx = collect_state.partition_idx;
                let collect_state = std::mem::replace(
                    state,
                    SortPartitionState::Merging(SortMergingState { partition_idx }),
                );
                let mut collect_state = match collect_state {
                    SortPartitionState::Collecting(state) => state,
                    _ => unreachable!(),
                };

                collect_state.collection.sort_unsorted()?;
                operator_state
                    .queue
                    .add_collection(collect_state.collection)?;

                // Trigger merge work.
                Ok(PollFinalize::NeedsDrain)
            }
            other => Err(RayexecError::new(format!(
                "Sort partition state in invalid state: {other:?}",
            ))),
        }
    }
}

impl Explainable for PhysicalSort {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Sort")
    }
}
