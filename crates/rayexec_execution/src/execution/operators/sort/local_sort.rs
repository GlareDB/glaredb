use crate::{
    execution::operators::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush},
    expr::PhysicalSortExpression,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::util::{
    merger::{IterState, KWayMerger, MergeResult},
    sort_keys::SortKeysExtractor,
    sorted_batch::{IndexSortedBatch, SortedIndicesIter},
};

#[derive(Debug)]
pub enum LocalSortPartitionState {
    /// Partition is accepting data for sorting.
    Consuming {
        /// Extract the sort keys from a batch.
        extractor: SortKeysExtractor,

        /// Batches that we sorted the row indices for.
        ///
        /// Batches are not sorted relative to each other.
        batches: Vec<IndexSortedBatch>,

        /// Waker on the pull side that tried to get a batch before we were done
        /// sorting this partition.
        pull_waker: Option<Waker>,
    },

    /// Partition is producing sorted data.
    Producing {
        /// Merger for merging all batches in this partition.
        merger: KWayMerger<SortedIndicesIter>,
    },
}

/// Physical operator for sorting batches within a partition.
#[derive(Debug)]
pub struct PhysicalLocalSort {
    exprs: Vec<PhysicalSortExpression>,
}

impl PhysicalLocalSort {
    pub fn new(exprs: Vec<PhysicalSortExpression>) -> Self {
        PhysicalLocalSort { exprs }
    }

    pub fn create_states(&self, partitions: usize) -> Vec<LocalSortPartitionState> {
        let extractor = SortKeysExtractor::new(&self.exprs);
        (0..partitions)
            .map(|_| LocalSortPartitionState::Consuming {
                extractor: extractor.clone(),
                batches: Vec::new(),
                pull_waker: None,
            })
            .collect()
    }
}

impl PhysicalOperator for PhysicalLocalSort {
    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::LocalSort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            LocalSortPartitionState::Consuming {
                extractor, batches, ..
            } => {
                let keys = extractor.sort_keys(&batch)?;

                // Produce the indices that would result in a sorted batches. We
                // can use these indices later to `interleave` rows once we want
                // to start returning sorted batches.
                let mut sort_indices: Vec<_> = (0..batch.num_rows()).collect();
                sort_indices.sort_by_key(|idx| keys.row(*idx).expect("row to exist"));

                let batch = IndexSortedBatch {
                    sort_indices,
                    keys,
                    batch,
                };
                batches.push(batch);

                Ok(PollPush::NeedsMore)
            }
            LocalSortPartitionState::Producing { .. } => {
                panic!("attempted to push to partition that's already produding data")
            }
        }
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        let state = match partition_state {
            PartitionState::LocalSort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            LocalSortPartitionState::Consuming {
                batches,
                pull_waker,
                ..
            } => {
                let pull_waker = pull_waker.take(); // Taken here to satisfy lifetime.

                // Initialize the merger with all the batches.
                let mut inputs = Vec::with_capacity(batches.len());

                let batches = std::mem::take(batches);

                // Filter out any batches that don't have rows, and add them to the merger inputs.
                for batch in batches
                    .into_iter()
                    .filter(|batch| batch.batch.num_rows() > 0)
                {
                    let (batch, iter) = batch.into_batch_and_iter();
                    inputs.push((Some(batch), IterState::Iterator(iter)));
                }

                let merger = KWayMerger::try_new(inputs)?;

                // Wake up thread waiting to pull.
                if let Some(waker) = pull_waker {
                    waker.wake()
                }

                // Update partition state to "producing" using the merger.
                *state = LocalSortPartitionState::Producing { merger };

                Ok(())
            }
            LocalSortPartitionState::Producing { .. } => {
                panic!("attempted to finalize partition that's already producing data")
            }
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let mut state = match partition_state {
            PartitionState::LocalSort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match &mut state {
            LocalSortPartitionState::Consuming { pull_waker, .. } => {
                // Partition still collecting data to sort.
                *pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
            LocalSortPartitionState::Producing { merger } => {
                loop {
                    // TODO: Configurable batch size.
                    match merger.try_merge(1024)? {
                        MergeResult::Batch(batch) => {
                            return Ok(PollPull::Batch(batch));
                        }
                        MergeResult::Exhausted => {
                            return Ok(PollPull::Exhausted);
                        }
                        MergeResult::NeedsInput(idx) => {
                            // We're merging all batch in this partition, and
                            // the merger already has everything, so we go ahead
                            // and mark this batch as complete.
                            merger.input_finished(idx);
                            // Continue to keep merging...
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::operators::test_util::{
        make_i32_batch, unwrap_poll_pull_batch, TestContext,
    };
    use std::sync::Arc;

    use super::*;

    fn create_states(operator: &PhysicalLocalSort, partitions: usize) -> Vec<PartitionState> {
        operator
            .create_states(partitions)
            .into_iter()
            .map(PartitionState::LocalSort)
            .collect()
    }

    #[test]
    fn sort_single_partition_desc_nulls_first() {
        let inputs = vec![
            make_i32_batch([8, 10, 8, 4]),
            make_i32_batch([2, 3]),
            make_i32_batch([9, 1, 7, -1]),
        ];

        let operator = Arc::new(PhysicalLocalSort::new(vec![PhysicalSortExpression {
            column: 0,
            desc: true,
            nulls_first: true,
        }]));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push all the inputs.
        let push_cx = TestContext::new();
        for input in inputs {
            let poll_push = push_cx
                .poll_push(&operator, &mut partition_states[0], &operator_state, input)
                .unwrap();
            assert_eq!(PollPush::NeedsMore, poll_push);
        }
        operator
            .finalize_push(&mut partition_states[0], &operator_state)
            .unwrap();

        // Now pull.
        let pull_cx = TestContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([10, 9, 8, 8, 7, 4, 3, 2, 1, -1]);
        assert_eq!(expected, output);
    }

    #[test]
    fn sort_single_partition_asc_nulls_first() {
        let inputs = vec![
            make_i32_batch([8, 10, 8, 4]),
            make_i32_batch([2, 3]),
            make_i32_batch([9, 1, 7, -1]),
        ];

        let operator = Arc::new(PhysicalLocalSort::new(vec![PhysicalSortExpression {
            column: 0,
            desc: false,
            nulls_first: true,
        }]));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push all the inputs.
        let push_cx = TestContext::new();
        for input in inputs {
            let poll_push = push_cx
                .poll_push(&operator, &mut partition_states[0], &operator_state, input)
                .unwrap();
            assert_eq!(PollPush::NeedsMore, poll_push);
        }
        operator
            .finalize_push(&mut partition_states[0], &operator_state)
            .unwrap();

        // Now pull.
        let pull_cx = TestContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([-1, 1, 2, 3, 4, 7, 8, 8, 9, 10]);
        assert_eq!(expected, output);
    }

    #[test]
    fn sort_single_partition_multiple_outputs() {
        let inputs = vec![
            make_i32_batch(0..1024),
            make_i32_batch(1024..2048),
            make_i32_batch(2048..3072),
        ];

        let operator = Arc::new(PhysicalLocalSort::new(vec![PhysicalSortExpression {
            column: 0,
            desc: true,
            nulls_first: true,
        }]));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push all the inputs.
        let push_cx = TestContext::new();
        for input in inputs {
            let poll_push = push_cx
                .poll_push(&operator, &mut partition_states[0], &operator_state, input)
                .unwrap();
            assert_eq!(PollPush::NeedsMore, poll_push);
        }
        operator
            .finalize_push(&mut partition_states[0], &operator_state)
            .unwrap();

        // Now pull.
        // TODO: Currently batch size is hard coded to 1024, we make assumptions
        // about the output size.
        let pull_cx = TestContext::new();

        let mut outputs = Vec::new();
        for _ in 0..3 {
            let poll_pull = pull_cx
                .poll_pull(&operator, &mut partition_states[0], &operator_state)
                .unwrap();
            let output = unwrap_poll_pull_batch(poll_pull);
            outputs.push(output);
        }

        let expected = vec![
            make_i32_batch((2048..3072).rev()),
            make_i32_batch((1024..2048).rev()),
            make_i32_batch((0..1024).rev()),
        ];

        assert_eq!(expected, outputs);

        // Make sure we're exhausted.
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Exhausted, poll_pull);
    }
}
