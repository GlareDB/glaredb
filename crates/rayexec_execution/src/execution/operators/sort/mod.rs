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
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::sort::partial_sort::{PartialSortedRowCollection, SortedRowAppendState};
use crate::arrays::sort::sort_layout::{SortColumn, SortLayout};
use crate::arrays::sort::sorted_run::{SortedRun, SortedRunScanState};
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
    /// Partition is draining a sorted run.
    ///
    /// Only a single partition will be set in this state.
    Draining(SortDrainingState),
    /// Partition is finished.
    Finished,
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
pub struct SortDrainingState {
    scan_state: SortedRunScanState,
    sorted_run: SortedRun<NopBufferManager>,
}

#[derive(Debug)]
pub struct SortOperatorState {
    /// Queue for sorting sorted runs.
    queue: MergeQueue,
}

#[derive(Debug)]
pub struct PhysicalSort {
    pub(crate) sort_exprs: Vec<PhysicalSortExpression>,
    pub(crate) output_types: Vec<DataType>,
}

impl ExecutableOperator for PhysicalSort {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Self::States> {
        let key_layout = SortLayout::new(self.sort_exprs.iter().map(|sort_expr| SortColumn {
            desc: sort_expr.desc,
            nulls_first: sort_expr.nulls_first,
            datatype: sort_expr.column.datatype(),
        }));
        // Assumes input and output types are the same.
        let data_layout = RowLayout::new(self.output_types.clone());

        let partition_states = (0..partitions)
            .map(|idx| {
                let evaluator = ExpressionEvaluator::try_new(
                    self.sort_exprs
                        .iter()
                        .map(|sort_expr| sort_expr.column.clone()),
                    batch_size,
                )?;

                let collection = PartialSortedRowCollection::new(
                    key_layout.clone(),
                    data_layout.clone(),
                    batch_size,
                );
                let append = collection.init_append_state();

                Ok(PartitionState::Sort(SortPartitionState::Collecting(
                    SortCollectingState {
                        partition_idx: idx,
                        evaluator,
                        keys: Batch::try_new(key_layout.datatypes(), batch_size)?,
                        collection,
                        append,
                    },
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        let merge_queue = MergeQueue::new(key_layout, data_layout, batch_size, partitions);
        let operator_state = OperatorState::Sort(SortOperatorState { queue: merge_queue });

        Ok(UnaryInputStates {
            operator_state,
            partition_states,
        })
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
            SortPartitionState::Merging(merge_state) => {
                // Make progress in merging.
                let poll = operator_state
                    .queue
                    .poll_merge_next(cx, merge_state.partition_idx)?;
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
                        match operator_state.queue.take_sorted_run()? {
                            Some(sorted_run) => {
                                // This partition is responsible for draining.
                                *state = SortPartitionState::Draining(SortDrainingState {
                                    scan_state: sorted_run.init_scan_state(),
                                    sorted_run,
                                });

                                // TODO: We could probably drop the key blocks
                                // here since we don't scan those.

                                // Dummy output, HasMore will trigger the real
                                // pull. Done just to centralize logical under
                                // the Draining branch.
                                let output = inout.output.required("output batch required")?;
                                output.set_num_rows(0)?;

                                Ok(PollExecute::HasMore)
                            }
                            None => {
                                // Some other partition is draining (or we're
                                // sorting zero rows). Partition is finished.
                                *state = SortPartitionState::Finished;
                                let output = inout.output.required("output batch required")?;
                                output.set_num_rows(0)?;

                                Ok(PollExecute::Exhausted)
                            }
                        }
                    }
                }
            }
            SortPartitionState::Draining(draining_state) => {
                let output = inout.output.required("output batch required")?;
                output.reset_for_write()?;

                let count = draining_state.sorted_run.scan_data(
                    &mut draining_state.scan_state,
                    &operator_state.queue.data_layout,
                    output,
                )?;

                if count == 0 {
                    return Ok(PollExecute::Exhausted);
                }

                Ok(PollExecute::HasMore)
            }
            SortPartitionState::Finished => {
                let output = inout.output.required("output batch required")?;
                output.set_num_rows(0)?;

                Ok(PollExecute::Exhausted)
            }
        }
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
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
