pub mod encode;

mod merge;
mod sort_data;
mod sort_layout;

use std::task::Context;

use merge::{MergeQueue, Merger};
use parking_lot::Mutex;
use rayexec_error::{OptionExt, RayexecError, Result};
use sort_data::{SortBlock, SortData};
use sort_layout::SortLayout;

use super::{ExecutableOperator, ExecuteInOutState, OperatorState, PartitionState, PollExecute, PollFinalize};
use crate::arrays::buffer_manager::NopBufferManager;
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
