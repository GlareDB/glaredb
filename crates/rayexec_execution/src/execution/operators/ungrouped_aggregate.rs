use crate::expr::PhysicalAggregateExpression;
use crate::functions::aggregate::{multi_array_drain, GroupedStates};
use crate::logical::explainable::{ExplainConfig, ExplainEntry};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::task::{Context, Waker};

use crate::logical::explainable::Explainable;

use super::{OperatorState, PartitionState, PhysicalOperator, PollFinalize, PollPull, PollPush};

#[derive(Debug)]
pub enum UngroupedAggregatePartitionState {
    /// Partition is currently aggregating.
    Aggregating {
        /// Index of this partition.
        partition_idx: usize,

        /// States for all aggregates with one group each, one per function call
        /// (SUM, AVG, etc).
        agg_states: Vec<Box<dyn GroupedStates>>,
    },

    /// Partition is currently producing.
    Producing {
        /// Index of this partition.
        partition_idx: usize,

        /// Batches this partition will output.
        ///
        /// Currently only one partition will actually produce output. The rest
        /// will be empty.
        batches: Vec<Batch>,
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
    agg_states: Vec<Box<dyn GroupedStates>>,

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

    pub fn create_states(
        &self,
        num_partitions: usize,
    ) -> (
        UngroupedAggregateOperatorState,
        Vec<UngroupedAggregatePartitionState>,
    ) {
        let inner = OperatorStateInner {
            remaining: num_partitions,
            agg_states: self.create_agg_states_with_single_group(),
            pull_wakers: (0..num_partitions).map(|_| None).collect(),
        };
        let operator_state = UngroupedAggregateOperatorState {
            inner: Mutex::new(inner),
        };

        let partition_states: Vec<_> = (0..num_partitions)
            .map(|idx| UngroupedAggregatePartitionState::Aggregating {
                partition_idx: idx,
                agg_states: self.create_agg_states_with_single_group(),
            })
            .collect();

        (operator_state, partition_states)
    }

    fn create_agg_states_with_single_group(&self) -> Vec<Box<dyn GroupedStates>> {
        let mut states = Vec::with_capacity(self.aggregates.len());
        for agg in &self.aggregates {
            let mut state = agg.function.new_grouped_state();
            state.new_group();
            states.push(state);
        }
        states
    }
}

impl PhysicalOperator for PhysicalUngroupedAggregate {
    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::UngroupedAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            UngroupedAggregatePartitionState::Aggregating { agg_states, .. } => {
                // All rows contribute to computing the aggregate.
                let row_selection = Bitmap::new_with_val(true, batch.num_rows());
                // All rows map to the same group (group 0)
                let mapping = vec![0; batch.num_rows()];

                for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                    let cols: Vec<_> = agg
                        .column_indices
                        .iter()
                        .map(|idx| batch.column(*idx).expect("column to exist").as_ref())
                        .collect();

                    agg_states[agg_idx].update_states(&row_selection, &cols, &mapping)?;
                }

                // Keep pushing.
                Ok(PollPush::NeedsMore)
            }
            UngroupedAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to push to partition that should be producing batches",
            )),
        }
    }

    fn poll_finalize_push(
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
                let mapping = vec![0];

                for (local_agg_state, global_agg_state) in
                    agg_states.into_iter().zip(shared.agg_states.iter_mut())
                {
                    global_agg_state.try_combine(local_agg_state, &mapping)?;
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

                    let mut batches = Vec::new();
                    while let Some(arrays) = multi_array_drain(&mut final_states, 1000)? {
                        let batch = Batch::try_new(arrays)?;
                        batches.push(batch);
                    }

                    // At least right now. Windows might end up doing something
                    // different.
                    assert_eq!(1, batches.len());

                    *state = UngroupedAggregatePartitionState::Producing {
                        partition_idx: *partition_idx,
                        batches,
                    }
                }

                Ok(PollFinalize::Finalized)
            }
            UngroupedAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to finalize push partition that's producing",
            )),
        }
    }

    fn poll_pull(
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
                Some(batch) => Ok(PollPull::Batch(batch)),
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
        ExplainEntry::new("PhysicalUngroupedAggregate").with_values("aggregates", &self.aggregates)
    }
}
