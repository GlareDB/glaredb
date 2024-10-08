use crate::database::DatabaseContext;
use crate::execution::operators::{ExecutionStates, InputOutputStates, PollFinalize};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;
use crate::{
    execution::operators::{ExecutableOperator, OperatorState, PartitionState, PollPull, PollPush},
    expr::physical::PhysicalSortExpression,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::sync::Arc;
use std::task::{Context, Waker};

use super::util::{
    merger::{IterState, KWayMerger, MergeResult},
    sort_keys::SortKeysExtractor,
    sorted_batch::{IndexSortedBatch, SortedIndicesIter},
};

#[derive(Debug)]
pub enum ScatterSortPartitionState {
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

/// Physical operator for sorting batches within a partition stream.
#[derive(Debug)]
pub struct PhysicalScatterSort {
    exprs: Vec<PhysicalSortExpression>,
}

impl PhysicalScatterSort {
    pub fn new(exprs: Vec<PhysicalSortExpression>) -> Self {
        PhysicalScatterSort { exprs }
    }
}

impl ExecutableOperator for PhysicalScatterSort {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        let extractor = SortKeysExtractor::new(&self.exprs);
        let states = (0..partitions)
            .map(|_| {
                PartitionState::ScatterSort(ScatterSortPartitionState::Consuming {
                    extractor: extractor.clone(),
                    batches: Vec::new(),
                    pull_waker: None,
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

    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::ScatterSort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            ScatterSortPartitionState::Consuming {
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
            ScatterSortPartitionState::Producing { .. } => {
                panic!("attempted to push to partition that's already produding data")
            }
        }
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::ScatterSort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            ScatterSortPartitionState::Consuming {
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
                *state = ScatterSortPartitionState::Producing { merger };

                Ok(PollFinalize::Finalized)
            }
            ScatterSortPartitionState::Producing { .. } => {
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
            PartitionState::ScatterSort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match &mut state {
            ScatterSortPartitionState::Consuming { pull_waker, .. } => {
                // Partition still collecting data to sort.
                *pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
            ScatterSortPartitionState::Producing { merger } => {
                loop {
                    // TODO: Configurable batch size.
                    match merger.try_merge(1024)? {
                        MergeResult::Batch(batch) => {
                            return Ok(PollPull::Computed(batch.into()));
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

impl Explainable for PhysicalScatterSort {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("LocalSort")
    }
}

impl DatabaseProtoConv for PhysicalScatterSort {
    type ProtoType = rayexec_proto::generated::execution::PhysicalLocalSort;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            exprs: self
                .exprs
                .iter()
                .map(|expr| expr.to_proto_ctx(context))
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            exprs: proto
                .exprs
                .into_iter()
                .map(|expr| DatabaseProtoConv::from_proto_ctx(expr, context))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        execution::operators::test_util::{
            make_i32_batch, test_database_context, unwrap_poll_pull_batch, TestWakerContext,
        },
        expr::physical::column_expr::PhysicalColumnExpr,
    };
    use std::sync::Arc;

    use super::*;

    fn create_states(operator: &PhysicalScatterSort, partitions: usize) -> Vec<PartitionState> {
        let context = test_database_context();
        let states = operator.create_states(&context, vec![partitions]).unwrap();

        match states.partition_states {
            InputOutputStates::OneToOne { partition_states } => partition_states,
            other => panic!("unexpected states: {other:?}"),
        }
    }

    #[test]
    fn sort_single_partition_desc_nulls_first() {
        let inputs = vec![
            make_i32_batch([8, 10, 8, 4]),
            make_i32_batch([2, 3]),
            make_i32_batch([9, 1, 7, -1]),
        ];

        let operator = Arc::new(PhysicalScatterSort::new(vec![PhysicalSortExpression {
            column: PhysicalColumnExpr { idx: 0 },
            desc: true,
            nulls_first: true,
        }]));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push all the inputs.
        let push_cx = TestWakerContext::new();
        for input in inputs {
            let poll_push = push_cx
                .poll_push(&operator, &mut partition_states[0], &operator_state, input)
                .unwrap();
            assert_eq!(PollPush::NeedsMore, poll_push);
        }
        operator
            .poll_finalize_push(
                &mut push_cx.context(),
                &mut partition_states[0],
                &operator_state,
            )
            .unwrap();

        // Now pull.
        let pull_cx = TestWakerContext::new();
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

        let operator = Arc::new(PhysicalScatterSort::new(vec![PhysicalSortExpression {
            column: PhysicalColumnExpr { idx: 0 },
            desc: false,
            nulls_first: true,
        }]));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push all the inputs.
        let push_cx = TestWakerContext::new();
        for input in inputs {
            let poll_push = push_cx
                .poll_push(&operator, &mut partition_states[0], &operator_state, input)
                .unwrap();
            assert_eq!(PollPush::NeedsMore, poll_push);
        }
        operator
            .poll_finalize_push(
                &mut push_cx.context(),
                &mut partition_states[0],
                &operator_state,
            )
            .unwrap();

        // Now pull.
        let pull_cx = TestWakerContext::new();
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

        let operator = Arc::new(PhysicalScatterSort::new(vec![PhysicalSortExpression {
            column: PhysicalColumnExpr { idx: 0 },
            desc: true,
            nulls_first: true,
        }]));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push all the inputs.
        let push_cx = TestWakerContext::new();
        for input in inputs {
            let poll_push = push_cx
                .poll_push(&operator, &mut partition_states[0], &operator_state, input)
                .unwrap();
            assert_eq!(PollPush::NeedsMore, poll_push);
        }
        operator
            .poll_finalize_push(
                &mut push_cx.context(),
                &mut partition_states[0],
                &operator_state,
            )
            .unwrap();

        // Now pull.
        // TODO: Currently batch size is hard coded to 1024, we make assumptions
        // about the output size.
        let pull_cx = TestWakerContext::new();

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
