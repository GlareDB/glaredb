mod encode;
mod merge;
mod sort_data;
mod sort_layout;

use std::task::Context;

use merge::{MergeQueue, Merger};
use parking_lot::Mutex;
use rayexec_error::{OptionExt, RayexecError, Result};
use sort_data::{SortBlock, SortData};
use sort_layout::SortLayout;

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::arrays::buffer_manager::NopBufferManager;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub enum SortPartitionState {
    /// Partition is consuming data, building up locally sorted blocks.
    Consume(PartitionStateLocalSort),
    /// Partition is producing data from all input partitions.
    ///
    /// Only a single partition will ever be in this state (to enforce data
    /// being globally sorted).
    Produce(PartitionStateGlobalSort),
    /// Partition is done producing any data.
    Finished,
}

#[derive(Debug)]
pub struct PartitionStateLocalSort {
    output_capacity: usize,
    /// Partition-local sort data.
    sort_data: SortData<NopBufferManager>,
}

#[derive(Debug)]
pub struct PartitionStateGlobalSort {
    /// Merger containing all sort blocks from all partitions.
    merger: Merger<NopBufferManager>,
}

#[derive(Debug)]
pub struct SortOperatorState {
    inner: Mutex<SortOperatorStateInner>,
}

#[derive(Debug)]
struct SortOperatorStateInner {
    /// Number of partitions we're still waiting on before we can start
    /// producing output.
    remaining: usize,
    /// Queues from each partition.
    ///
    /// Each queue contains totally ordered blocks within that queue. These will
    /// be the inputs to the final global merge.
    ///
    /// Pushed to when a partition is finalized.
    queues: Vec<MergeQueue<NopBufferManager>>,
}

#[derive(Debug)]
pub struct PhysicalSort {
    pub(crate) layout: SortLayout,
}

impl ExecutableOperator for PhysicalSort {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        let operator_state = OperatorState::Sort(SortOperatorState {
            inner: Mutex::new(SortOperatorStateInner {
                remaining: partitions,
                queues: Vec::new(),
            }),
        });

        let partition_states = (0..partitions)
            .map(|_| {
                PartitionState::Sort(SortPartitionState::Consume(PartitionStateLocalSort {
                    output_capacity: batch_size,
                    sort_data: SortData::new(batch_size),
                }))
            })
            .collect();

        Ok(PartitionAndOperatorStates::Branchless {
            operator_state,
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
            PartitionState::Sort(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        match state {
            SortPartitionState::Consume(state) => {
                let batch = inout.input.required("input batch required")?;
                state.sort_data.push_batch(&NopBufferManager, &self.layout, batch)?;

                // TODO: Threshold begin sort.

                Ok(PollExecute::NeedsMore)
            }
            SortPartitionState::Produce(state) => {
                let out = inout.output.required("output batch required")?;
                let count = state.merger.merge_round(out)?;

                // Update output batch with correct number of rows.
                out.set_num_rows(count)?;

                if out.num_rows() == 0 {
                    Ok(PollExecute::Exhausted)
                } else {
                    Ok(PollExecute::HasMore)
                }
            }
            SortPartitionState::Finished => Ok(PollExecute::Exhausted),
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

        match state {
            SortPartitionState::Consume(consume_state) => {
                // Ensure local sort data is completely sorted with blocks.
                consume_state
                    .sort_data
                    .sort_unsorted_blocks(&NopBufferManager, &self.layout)?;

                // Create a queue per block.
                let blocks = consume_state.sort_data.take_sorted_for_merge();
                let queues: Vec<_> = blocks
                    .into_iter()
                    .flat_map(|block| MergeQueue::new_single(block))
                    .collect();

                let mut merger = Merger::new(queues);

                // Merge until we run out of mergeable rows.
                let mut blocks = Vec::new();
                loop {
                    let mut block = SortBlock::new(&NopBufferManager, &self.layout, consume_state.output_capacity)?;
                    let count = merger.merge_round(&mut block)?;
                    block.block.set_row_count(count)?;

                    if count == 0 {
                        // No rows, we're done merging.
                        break;
                    }

                    blocks.push(block);
                }

                let mut operator_state = match operator_state {
                    OperatorState::Sort(state) => state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                let partition_queue = MergeQueue::new(blocks);
                if let Some(queue) = partition_queue {
                    operator_state.queues.push(queue);
                }

                operator_state.remaining -= 1;

                // If we're the last partition, take the queues and start the
                // global merge.
                if operator_state.remaining == 0 {
                    let queues = std::mem::take(&mut operator_state.queues);
                    let global_merger = Merger::new(queues);

                    *state = SortPartitionState::Produce(PartitionStateGlobalSort { merger: global_merger });

                    return Ok(PollFinalize::NeedsDrain);
                }

                // Otherwise this partition's finished.
                Ok(PollFinalize::Finalized)
            }
            SortPartitionState::Produce(_) => Err(RayexecError::new("cannot finalize partition that's producing")),
            SortPartitionState::Finished => Err(RayexecError::new("cannot finalize partition that's finished")),
        }
    }
}

impl Explainable for PhysicalSort {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::{assert_batches_eq, new_batch_from_arrays, new_i32_array, new_string_array};
    use crate::execution::operators_exp::testutil::context::test_database_context;
    use crate::execution::operators_exp::testutil::wrapper::OperatorWrapper;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::physical::PhysicalSortExpression;

    #[test]
    fn sort_single_partition_sort_on_i32() {
        let operator = PhysicalSort {
            layout: SortLayout::new(
                vec![DataType::Int32, DataType::Utf8],
                &[PhysicalSortExpression {
                    column: PhysicalColumnExpr { idx: 0 },
                    desc: false,
                    nulls_first: false,
                }],
            ),
        };

        let context = test_database_context();
        let (op_state, mut part_states) = operator
            .create_states(&context, 4, 1)
            .unwrap()
            .branchless_into_states()
            .unwrap();

        let operator = OperatorWrapper::new(operator);

        let inputs = [
            new_batch_from_arrays([new_i32_array([4, 5, 6]), new_string_array(["a", "b", "c"])]),
            new_batch_from_arrays([new_i32_array([1, 2, 7]), new_string_array(["d", "e", "f"])]),
        ];

        for mut input in inputs {
            let poll = operator
                .poll_execute(
                    &mut part_states[0],
                    &op_state,
                    ExecuteInOutState {
                        input: Some(&mut input),
                        output: None,
                    },
                )
                .unwrap();

            assert_eq!(PollExecute::NeedsMore, poll);
        }

        let poll = operator.poll_finalize(&mut part_states[0], &op_state).unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let mut out = Batch::new(&NopBufferManager, [DataType::Int32, DataType::Utf8], 4).unwrap();
        let poll = operator
            .poll_execute(
                &mut part_states[0],
                &op_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expect1 = new_batch_from_arrays([new_i32_array([1, 2, 4, 5]), new_string_array(["d", "e", "a", "b"])]);
        assert_batches_eq(&expect1, &out);

        let poll = operator
            .poll_execute(
                &mut part_states[0],
                &op_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expect2 = new_batch_from_arrays([new_i32_array([6, 7]), new_string_array(["c", "f"])]);
        assert_batches_eq(&expect2, &out);

        let poll = operator
            .poll_execute(
                &mut part_states[0],
                &op_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);
    }
}
