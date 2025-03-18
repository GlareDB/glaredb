mod merge_queue;

use std::task::Context;

use glaredb_error::{DbError, Result};
use merge_queue::{MergeQueue, PollMerge};

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::sort::partial_sort::{PartialSortedRowCollection, SortedRowAppendState};
use crate::arrays::sort::sort_layout::{SortColumn, SortLayout};
use crate::arrays::sort::sorted_segment::{SortedSegment, SortedSegmentScanState};
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
    scan_state: SortedSegmentScanState,
    sorted_run: SortedSegment,
}

impl SortDrainingState {
    /// Drains into the next batch.
    ///
    /// Returns a bool indicating if we should keep polling for more output.
    fn drain_next(&mut self, op_state: &SortOperatorState, output: &mut Batch) -> Result<bool> {
        output.reset_for_write()?;

        let count =
            self.sorted_run
                .scan_data(&mut self.scan_state, &op_state.queue.data_layout, output)?;

        if count == 0 {
            return Ok(false);
        }

        Ok(true)
    }
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

impl PhysicalSort {
    pub fn new(sort_exprs: Vec<PhysicalSortExpression>, output_types: Vec<DataType>) -> Self {
        PhysicalSort {
            sort_exprs,
            output_types,
        }
    }
}

impl BaseOperator for PhysicalSort {
    type OperatorState = SortOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let key_layout = SortLayout::new(self.sort_exprs.iter().map(|sort_expr| SortColumn {
            desc: sort_expr.desc,
            nulls_first: sort_expr.nulls_first,
            datatype: sort_expr.column.datatype(),
        }));
        // Assumes input and output types are the same.
        let data_layout = RowLayout::new(self.output_types.clone());

        Ok(SortOperatorState {
            queue: MergeQueue::new(key_layout, data_layout, props.batch_size),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl ExecuteOperator for PhysicalSort {
    type PartitionExecuteState = SortPartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let states = (0..partitions)
            .map(|idx| {
                let evaluator = ExpressionEvaluator::try_new(
                    self.sort_exprs
                        .iter()
                        .map(|sort_expr| sort_expr.column.clone()),
                    props.batch_size,
                )?;

                let collection = PartialSortedRowCollection::new(
                    operator_state.queue.key_layout.clone(),
                    operator_state.queue.data_layout.clone(),
                    props.batch_size,
                );
                let append = collection.init_append_state();

                Ok(SortPartitionState::Collecting(SortCollectingState {
                    partition_idx: idx,
                    evaluator,
                    keys: Batch::new(
                        operator_state.queue.key_layout.datatypes(),
                        props.batch_size,
                    )?,
                    collection,
                    append,
                }))
            })
            .collect::<Result<Vec<_>>>()?;

        // Notify merge queue of how many partitions we're merging.
        operator_state.queue.prepare_for_partitions(partitions);

        Ok(states)
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        match state {
            SortPartitionState::Collecting(state) => {
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
                                let mut drain_state = SortDrainingState {
                                    scan_state: sorted_run.init_scan_state(),
                                    sorted_run,
                                };

                                // TODO: We could probably drop the key blocks
                                // here since we don't scan those.

                                let has_more = drain_state.drain_next(operator_state, output)?;
                                if has_more {
                                    *state = SortPartitionState::Draining(drain_state);
                                    Ok(PollExecute::HasMore)
                                } else {
                                    *state = SortPartitionState::Finished;
                                    Ok(PollExecute::Exhausted)
                                }
                            }
                            None => {
                                // Some other partition is draining (or we're
                                // sorting zero rows). Partition is finished.
                                *state = SortPartitionState::Finished;
                                output.set_num_rows(0)?;

                                Ok(PollExecute::Exhausted)
                            }
                        }
                    }
                }
            }
            SortPartitionState::Draining(drain_state) => {
                let has_more = drain_state.drain_next(operator_state, output)?;
                if has_more {
                    Ok(PollExecute::HasMore)
                } else {
                    *state = SortPartitionState::Finished;
                    Ok(PollExecute::Exhausted)
                }
            }
            SortPartitionState::Finished => {
                output.set_num_rows(0)?;
                Ok(PollExecute::Exhausted)
            }
        }
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
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
                    .add_sorted_partition(collect_state.collection)?;

                // Trigger merge work.
                Ok(PollFinalize::NeedsDrain)
            }
            other => Err(DbError::new(format!(
                "Sort partition state in invalid state: {other:?}",
            ))),
        }
    }
}

impl Explainable for PhysicalSort {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Sort")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::exprs::plan_scalar;
    use crate::testutil::operator::OperatorWrapper;
    use crate::{expr, generate_batch};

    fn new_sort_operator(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpression>,
        output_types: impl IntoIterator<Item = DataType>,
        num_partitions: usize,
    ) -> (
        OperatorWrapper<PhysicalSort>,
        SortOperatorState,
        Vec<SortPartitionState>,
    ) {
        let wrapper = OperatorWrapper::new(PhysicalSort::new(
            sort_exprs.into_iter().collect(),
            output_types.into_iter().collect(),
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, num_partitions)
            .unwrap();

        (wrapper, op_state, states)
    }

    #[test]
    fn sort_single_column_single_partition() {
        // SORT: c1
        let mut list = TableList::empty();
        let t0 = list.push_table(None, [DataType::Int32], ["c1"]).unwrap();

        let sort_exprs = [PhysicalSortExpression {
            column: plan_scalar(&list, expr::column((t0, 0), DataType::Int32)),
            desc: false,
            nulls_first: false,
        }];

        let (wrapper, op_state, mut states) = new_sort_operator(sort_exprs, [DataType::Int32], 1);

        let mut output = Batch::new([DataType::Int32], 16).unwrap();

        let mut input = generate_batch!([4, 3, 1, 5]);
        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut states[0])
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        // TODO: This should change to exhausted. Currently the next poll will
        // return exhausted with a batch of zero rows.
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([1, 3, 4, 5]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);
        assert_eq!(0, output.num_rows);
    }

    #[test]
    fn sort_single_column_multiple_partitions() {
        // SORT: c1
        let mut list = TableList::empty();
        let t0 = list.push_table(None, [DataType::Int32], ["c1"]).unwrap();

        let sort_exprs = [PhysicalSortExpression {
            column: plan_scalar(&list, expr::column((t0, 0), DataType::Int32)),
            desc: false,
            nulls_first: false,
        }];

        let (wrapper, op_state, mut states) = new_sort_operator(sort_exprs, [DataType::Int32], 4);

        let mut outputs = [
            Batch::new([DataType::Int32], 16).unwrap(),
            Batch::new([DataType::Int32], 16).unwrap(),
            Batch::new([DataType::Int32], 16).unwrap(),
            Batch::new([DataType::Int32], 16).unwrap(),
        ];

        // Push batches for each partition.
        let mut inputs = [
            generate_batch!([1, 2, 3, 4]),
            generate_batch!([6, 3, 1, 8]),
            generate_batch!([3, 3, 3, 3]),
            generate_batch!([100, 100, 100, 100]),
        ];

        for idx in 0..4 {
            let poll = wrapper
                .poll_execute(
                    &op_state,
                    &mut states[idx],
                    &mut inputs[idx],
                    &mut outputs[idx],
                )
                .unwrap();
            assert_eq!(PollExecute::NeedsMore, poll);
        }

        // Finalize each batch. All partitions will receive NeedsDrain,
        // triggering execution work for the merging.
        for idx in 0..4 {
            let poll = wrapper
                .poll_finalize_execute(&op_state, &mut states[idx])
                .unwrap();
            assert_eq!(PollFinalize::NeedsDrain, poll);
        }

        let mut output_checked = false;
        let expected = generate_batch!([1, 1, 2, 3, 3, 3, 3, 3, 3, 4, 6, 8, 100, 100, 100, 100]);

        // Now execute until all are exhausted.
        let mut queue: VecDeque<_> = (0..4).collect();
        while let Some(idx) = queue.pop_front() {
            let poll = wrapper
                .poll_execute(
                    &op_state,
                    &mut states[idx],
                    &mut inputs[idx],
                    &mut outputs[idx],
                )
                .unwrap();

            match poll {
                PollExecute::Pending => queue.push_back(idx),
                PollExecute::Exhausted | PollExecute::HasMore => {
                    // Only one partition should receive the fully sorted
                    // output.
                    if outputs[idx].num_rows() > 0 {
                        if output_checked {
                            panic!("multiple partitions received output");
                        }

                        assert_batches_eq(&expected, &outputs[idx]);
                        output_checked = true;
                    }
                }
                other => panic!("unexpected poll: {other:?}"),
            }
            if poll == PollExecute::Pending {
                queue.push_back(idx);
            }
        }
    }

    #[test]
    fn sort_single_column_with_expr_single_partition() {
        // Same as above, but with the sort key being more than just a column
        // expression.
        //
        // SORT: c1 + 8
        // DATA: c1
        let mut list = TableList::empty();
        let t0 = list.push_table(None, [DataType::Int32], ["c1"]).unwrap();

        let sort_exprs = [PhysicalSortExpression {
            column: plan_scalar(
                &list,
                expr::add(expr::column((t0, 0), DataType::Int32), expr::lit(8)).unwrap(),
            ),
            desc: false,
            nulls_first: false,
        }];

        let (wrapper, op_state, mut states) = new_sort_operator(sort_exprs, [DataType::Int32], 1);

        let mut output = Batch::new([DataType::Int32], 16).unwrap();

        let mut input = generate_batch!([4, 3, 1, 5]);
        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut states[0])
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        // TODO: This should change to exhausted. Currently the next poll will
        // return exhausted with a batch of zero rows.
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([1, 3, 4, 5]);
        assert_batches_eq(&expected, &output);
    }
}
