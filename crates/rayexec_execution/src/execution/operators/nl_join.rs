use parking_lot::Mutex;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::compute::filter::filter;
use rayexec_bullet::compute::take::take;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::task::Context;
use std::{sync::Arc, task::Waker};

use crate::expr::PhysicalScalarExpression;

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

/// Partition-local state on the build side.
#[derive(Debug, Default)]
pub struct NlJoinBuildPartitionState {
    /// All batches on the build side for a single partition.
    ///
    /// For hash joins, this would be a partition-local hash map.
    batches: Vec<Batch>,
}

/// Partition-local state on the probe side.
#[derive(Debug)]
pub struct NlJoinProbePartitionState {
    /// Partition index this state is for.
    ///
    /// This is needed to properly emplace a probe-side waker in the operator
    /// state if the join is still in the building phase. We only want to store
    /// the latest waker per partition, but we need to be able to store wakers
    /// from any of the probe partitions.
    partition_idx: usize,

    /// All batches from all partitions received on the build side.
    ///
    /// Store in the probe side local state to avoid needing to lock.
    all_batches: Arc<Vec<Batch>>,

    /// Bool for determining if `all_batches` has been populated from the global
    /// operator state.
    ///
    /// If false, the operator state needs to be referenced.
    ///
    /// This is separate bool as we can't determine anything from the length of
    /// `all_batches` as it's possible for a query to actually have no batches
    /// from the build side.
    is_populated: bool,

    /// Buffered batches that are ready to be pulled.
    buffered: VecDeque<Batch>,

    /// Push waker for if buffered isn't empty.
    ///
    /// We want to only continue pushing once the buffered batches are drained
    /// for back pressure.
    ///
    /// Woken when buffered is empty.
    push_waker: Option<Waker>,

    /// Pull waker for if buffered is empty.
    ///
    /// Woken once batches are put into buffered.
    pull_waker: Option<Waker>,

    /// If we should no longer be expected any inputs for this partition.
    input_finished: bool,
}

impl NlJoinProbePartitionState {
    pub fn new_for_partition(partition: usize) -> Self {
        NlJoinProbePartitionState {
            partition_idx: partition,
            all_batches: Arc::new(Vec::new()),
            is_populated: false,
            buffered: VecDeque::new(),
            push_waker: None,
            pull_waker: None,
            input_finished: false,
        }
    }
}

/// State shared across all partitions on both the build and probe side.
#[derive(Debug)]
pub struct NlJoinOperatorState {
    inner: Mutex<NlJoinOperatorStateInner>,
}

impl NlJoinOperatorState {
    /// Create a new operator state using the configured number of partitions.
    ///
    /// Build and probe side partition numbers may be different.
    pub fn new(num_partitions_build_side: usize, num_partitions_probe_side: usize) -> Self {
        NlJoinOperatorState {
            inner: Mutex::new(NlJoinOperatorStateInner::Building {
                batches: Vec::new(),
                build_partitions_remaining: num_partitions_build_side,
                probe_side_wakers: vec![None; num_partitions_probe_side],
            }),
        }
    }
}

#[derive(Debug)]
enum NlJoinOperatorStateInner {
    /// Join is currently waiting for all partitions on the build side to
    /// compete.
    Building {
        /// Build sides partitions write their batches here once they're done
        /// building.
        batches: Vec<Batch>,

        /// Number of partitions we're still waiting to complete on the build
        /// side.
        build_partitions_remaining: usize,

        /// Wakers for partitions on the probe side.
        ///
        /// This is indexed by partition index on the probe side.
        ///
        /// Initially this a vec of Nones and is only populated if a partition
        /// on the probe side attempts to push prior to us completing the build.
        probe_side_wakers: Vec<Option<Waker>>,
    },

    /// Build is complete, we're now in the probing phase.
    Probing {
        /// All batches from all partitions.
        batches: Arc<Vec<Batch>>,
    },
}

impl NlJoinOperatorStateInner {
    /// Transitions the state from `Building` to `Probing`.
    ///
    /// Must be called when number of partitions remaining on the build side is
    /// zero.
    fn into_probing(&mut self) {
        match self {
            Self::Building {
                batches,
                build_partitions_remaining,
                probe_side_wakers,
            } => {
                assert_eq!(0, *build_partitions_remaining);

                // Wake any pending probers.
                for waker in probe_side_wakers {
                    if let Some(waker) = waker.take() {
                        waker.wake();
                    }
                }

                let batches = std::mem::take(batches);
                *self = Self::Probing {
                    batches: Arc::new(batches),
                }
            }
            Self::Probing { .. } => panic!("inner state is already probing"),
        }
    }
}

/// Nested loop join.
#[derive(Debug)]
pub struct PhysicalNlJoin {
    /// Filter to apply after cross joining batches.
    filter: Option<PhysicalScalarExpression>,
}

impl PhysicalNlJoin {
    pub fn new(filter: Option<PhysicalScalarExpression>) -> Self {
        PhysicalNlJoin { filter }
    }
}

impl PhysicalOperator for PhysicalNlJoin {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::NlJoinBuild(state) => {
                state.batches.push(batch);
                Ok(PollPush::Pushed)
            }
            PartitionState::NlJoinProbe(state) => {
                // Check that the partition-local state has a reference to the
                // global vec of batches.
                if !state.is_populated {
                    // Need to get the global reference.
                    let operator_state = match operator_state {
                        OperatorState::NlJoin(operater_state) => operater_state,
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    let mut inner = operator_state.inner.lock();
                    match &mut *inner {
                        NlJoinOperatorStateInner::Building {
                            probe_side_wakers, ..
                        } => {
                            // Still waiting for the build to finish, register
                            // ourselves for a later wakeup when the build is
                            // complete.
                            probe_side_wakers[state.partition_idx] = Some(cx.waker().clone());
                            return Ok(PollPush::Pending(batch));
                        }
                        NlJoinOperatorStateInner::Probing { batches } => {
                            // Otherwise the batches are ready for us. Clone the
                            // reference into our local state.
                            state.all_batches = batches.clone();
                            state.is_populated = true;

                            // Continue...
                        }
                    }
                }

                // If we still have buffered batches, reschedule the push until
                // it's empty.
                if !state.buffered.is_empty() {
                    state.push_waker = Some(cx.waker().clone());
                    return Ok(PollPush::Pending(batch));
                }

                // Do the join.
                let mut batches = Vec::new();
                for left in state.all_batches.iter() {
                    let mut out = cross_join(left, &batch, self.filter.as_ref())?;
                    batches.append(&mut out);
                }

                state.buffered.extend(batches.into_iter());

                // We have stuff in the buffer, wake up the puller.
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollPush::Pushed)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()> {
        match partition_state {
            PartitionState::NlJoinBuild(state) => {
                let operator_state = match operator_state {
                    OperatorState::NlJoin(operater_state) => operater_state,
                    other => panic!("invalid operator state: {other:?}"),
                };

                let mut inner = operator_state.inner.lock();
                match &mut *inner {
                    NlJoinOperatorStateInner::Building {
                        batches,
                        build_partitions_remaining,
                        ..
                    } => {
                        // Put our batches into the global state.
                        batches.append(&mut state.batches);

                        // Probing no longer waiting on this partition.
                        *build_partitions_remaining -= 1;

                        // If we're the last build partition, go ahead and
                        // transition the global state to begin probing.
                        if *build_partitions_remaining == 0 {
                            inner.into_probing();
                        }

                        // And we're done.
                        Ok(())
                    }
                    other => panic!("inner join state is not building: {other:?}"),
                }
            }
            PartitionState::NlJoinProbe(state) => {
                state.input_finished = true;
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }
                Ok(())
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::NlJoinProbe(state) => {
                match state.buffered.pop_front() {
                    Some(batch) => Ok(PollPull::Batch(batch)),
                    None => {
                        if state.input_finished {
                            Ok(PollPull::Exhausted)
                        } else {
                            // We just gotta wait for more input.
                            if let Some(waker) = state.push_waker.take() {
                                waker.wake();
                            }
                            state.pull_waker = Some(cx.waker().clone());
                            Ok(PollPull::Pending)
                        }
                    }
                }
            }
            PartitionState::NlJoinBuild(_) => {
                // We should never attempt to pull with the build state. Builds
                // just act as a "sink" into the join. The probe is the proper
                // push/pull operator that happens to wait on the build to
                // complete.
                //
                // This case is separate just to make that clear.
                panic!("cannot pull with the build join state");
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

/// Generate a cross product of two batches, applying an optional filter to the
/// result.
fn cross_join(
    left: &Batch,
    right: &Batch,
    filter_expr: Option<&PhysicalScalarExpression>,
) -> Result<Vec<Batch>> {
    let mut batches = Vec::with_capacity(left.num_rows() * right.num_rows());

    // For each row in the left batch, join the entirety of right.
    for left_idx in 0..left.num_rows() {
        let left_indices = vec![left_idx; right.num_rows()];

        let mut cols = Vec::new();
        for col in left.columns() {
            // TODO: It'd be nice to have a logical repeated array type instead
            // of having to physically copy the same element `n` times into a
            // new array.
            let left_repeated = take(&col, &left_indices)?;
            cols.push(Arc::new(left_repeated));
        }

        // Join all of right.
        cols.extend_from_slice(right.columns());
        let mut batch = Batch::try_new(cols)?;

        // If we have a filter, apply it to the intermediate batch.
        if let Some(filter_expr) = &filter_expr {
            let arr = filter_expr.eval(&batch)?;
            let selection = match arr.as_ref() {
                Array::Boolean(arr) => arr,
                other => {
                    return Err(RayexecError::new(format!(
                        "Expected filter predicate in cross join to return a boolean, got {}",
                        other.datatype()
                    )))
                }
            };

            let filtered = batch
                .columns()
                .iter()
                .map(|col| filter(col, selection))
                .collect::<Result<Vec<_>, _>>()?;

            batch = Batch::try_new(filtered)?;
        }

        batches.push(batch);
    }

    Ok(batches)
}
