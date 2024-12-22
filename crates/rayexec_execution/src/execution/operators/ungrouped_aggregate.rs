use std::fmt::Debug;
use std::sync::Arc;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_bullet::batch::BatchOld;
use rayexec_error::{RayexecError, Result};

use super::hash_aggregate::distinct::DistinctGroupedStates;
use super::hash_aggregate::hash_table::GroupAddress;
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
use crate::execution::operators::InputOutputStates;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalAggregateExpression;
use crate::functions::aggregate::states::AggregateGroupStates;
use crate::functions::aggregate::ChunkGroupAddressIter;
use crate::proto::DatabaseProtoConv;

#[derive(Debug)]
pub enum UngroupedAggregatePartitionState {
    /// Partition is currently aggregating.
    Aggregating {
        /// Index of this partition.
        partition_idx: usize,

        /// States for all aggregates with one group each, one per function call
        /// (SUM, AVG, etc).
        agg_states: Vec<Box<dyn AggregateGroupStates>>,
    },

    /// Partition is currently producing.
    Producing {
        /// Index of this partition.
        partition_idx: usize,

        /// Batches this partition will output.
        ///
        /// Currently only one partition will actually produce output. The rest
        /// will be empty.
        batches: Vec<BatchOld>,
    },
}

#[derive(Debug)]
pub struct UngroupedAggregateOperatorState {
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
struct OperatorStateInner {
    /// Number of partitions remaining.
    remaining: usize,

    /// Global agg states that all partition will combine their local states
    /// with.
    agg_states: Vec<Box<dyn AggregateGroupStates>>,

    /// Pull side wakers for if we're still aggregating.
    ///
    /// Indexed by partition_idx
    pull_wakers: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate {
    /// Aggregates we're computing.
    ///
    /// Used to create the initial states.
    aggregates: Vec<PhysicalAggregateExpression>,
}

impl PhysicalUngroupedAggregate {
    pub fn new(aggregates: Vec<PhysicalAggregateExpression>) -> Self {
        PhysicalUngroupedAggregate { aggregates }
    }

    fn create_agg_states_with_single_group(&self) -> Result<Vec<Box<dyn AggregateGroupStates>>> {
        let mut states = Vec::with_capacity(self.aggregates.len());
        for agg in &self.aggregates {
            let mut state = if agg.is_distinct {
                Box::new(DistinctGroupedStates::new(
                    agg.function.function_impl.new_states(),
                ))
            } else {
                agg.function.function_impl.new_states()
            };
            state.new_states(1);
            states.push(state);
        }

        Ok(states)
    }
}

impl ExecutableOperator for PhysicalUngroupedAggregate {
    fn create_states_old(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let num_partitions = partitions[0];

        let inner = OperatorStateInner {
            remaining: num_partitions,
            agg_states: self.create_agg_states_with_single_group()?,
            pull_wakers: (0..num_partitions).map(|_| None).collect(),
        };
        let operator_state = UngroupedAggregateOperatorState {
            inner: Mutex::new(inner),
        };

        let partition_states = (0..num_partitions)
            .map(|idx| {
                Ok(PartitionState::UngroupedAggregate(
                    UngroupedAggregatePartitionState::Aggregating {
                        partition_idx: idx,
                        agg_states: self.create_agg_states_with_single_group()?,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::UngroupedAggregate(operator_state)),
            partition_states: InputOutputStates::OneToOne { partition_states },
        })
    }

    fn poll_push_old(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: BatchOld,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::UngroupedAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            UngroupedAggregatePartitionState::Aggregating { agg_states, .. } => {
                // All rows map to the same group (group 0)
                let addrs: Vec<_> = (0..batch.num_rows())
                    .map(|_| GroupAddress {
                        chunk_idx: 0,
                        row_idx: 0,
                    })
                    .collect();

                for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                    let cols: Vec<_> = agg
                        .columns
                        .iter()
                        .map(|expr| batch.column(expr.idx).expect("column to exist"))
                        .collect();

                    agg_states[agg_idx]
                        .update_states(&cols, ChunkGroupAddressIter::new(0, &addrs))?;
                }

                // Keep pushing.
                Ok(PollPush::NeedsMore)
            }
            UngroupedAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to push to partition that should be producing batches",
            )),
        }
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::UngroupedAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            UngroupedAggregatePartitionState::Aggregating {
                partition_idx,
                agg_states,
            } => {
                let agg_states = std::mem::take(agg_states);

                let mut shared = match operator_state {
                    OperatorState::UngroupedAggregate(state) => state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                // Everything maps to the same group (group 0)
                let mapping = [GroupAddress {
                    chunk_idx: 0,
                    row_idx: 0,
                }];

                for (mut local_agg_state, global_agg_state) in
                    agg_states.into_iter().zip(shared.agg_states.iter_mut())
                {
                    global_agg_state.combine(
                        &mut local_agg_state,
                        ChunkGroupAddressIter::new(0, &mapping),
                    )?;
                }

                shared.remaining -= 1;

                if shared.remaining == 0 {
                    // This partition is the chosen one to produce the output.
                    let mut final_states = std::mem::take(&mut shared.agg_states);

                    // Wake up other partitions to let them know they are not
                    // the chosen ones.
                    for waker in shared.pull_wakers.iter_mut() {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }

                    // Lock no longer needed.
                    std::mem::drop(shared);

                    let arrays = final_states
                        .iter_mut()
                        .map(|s| s.finalize())
                        .collect::<Result<Vec<_>>>()?;

                    let batch = BatchOld::try_new(arrays)?;

                    *state = UngroupedAggregatePartitionState::Producing {
                        partition_idx: *partition_idx,
                        batches: vec![batch],
                    }
                }

                Ok(PollFinalize::Finalized)
            }
            UngroupedAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to finalize push partition that's producing",
            )),
        }
    }

    fn poll_pull_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::UngroupedAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            UngroupedAggregatePartitionState::Producing { batches, .. } => match batches.pop() {
                Some(batch) => Ok(PollPull::Computed(batch.into())),
                None => Ok(PollPull::Exhausted),
            },
            UngroupedAggregatePartitionState::Aggregating { partition_idx, .. } => {
                let mut shared = match operator_state {
                    OperatorState::UngroupedAggregate(state) => state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                if shared.remaining == 0 {
                    // We weren't the chosen partition to produce output. Immediately exhausted.
                    return Ok(PollPull::Exhausted);
                }

                shared.pull_wakers[*partition_idx] = Some(cx.waker().clone());

                Ok(PollPull::Pending)
            }
        }
    }
}

impl Explainable for PhysicalUngroupedAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalUngroupedAggregate")
    }
}

impl DatabaseProtoConv for PhysicalUngroupedAggregate {
    type ProtoType = rayexec_proto::generated::execution::PhysicalUngroupedAggregate;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            aggregates: self
                .aggregates
                .iter()
                .map(|a| a.to_proto_ctx(context))
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            aggregates: proto
                .aggregates
                .into_iter()
                .map(|a| PhysicalAggregateExpression::from_proto_ctx(a, context))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}
