use std::sync::Arc;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_error::Result;

use super::util::outer_join_tracker::LeftOuterJoinTracker;
use super::ComputedBatches;
use crate::arrays::batch::Batch;
use crate::arrays::selection::SelectionVector;
use crate::database::DatabaseContext;
use crate::execution::operators::{
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::logical::logical_join::JoinType;
use crate::proto::DatabaseProtoConv;

/// Partition-local state on the build side.
#[derive(Debug, Default)]
pub struct NestedLoopJoinBuildPartitionState {
    /// All batches on the build side for a single partition.
    ///
    /// For hash joins, this would be a partition-local hash map.
    batches: Vec<Batch>,
}

/// Partition-local state on the probe side.
#[derive(Debug)]
pub struct NestedLoopJoinProbePartitionState {
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

    /// Buffered batch that's ready to be pulled.
    buffered: ComputedBatches,

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

    /// Track rows visited on the left side for this partition.
    partition_outer_join_tracker: Option<LeftOuterJoinTracker>,
}

impl NestedLoopJoinProbePartitionState {
    pub fn new_for_partition(partition: usize) -> Self {
        NestedLoopJoinProbePartitionState {
            partition_idx: partition,
            all_batches: Arc::new(Vec::new()),
            is_populated: false,
            buffered: ComputedBatches::None,
            push_waker: None,
            pull_waker: None,
            input_finished: false,
            partition_outer_join_tracker: None,
        }
    }
}

/// State shared across all partitions on both the build and probe side.
#[derive(Debug)]
pub struct NestedLoopJoinOperatorState {
    inner: Mutex<SharedOperatorState>,
}

impl NestedLoopJoinOperatorState {
    /// Create a new operator state using the configured number of partitions.
    ///
    /// Build and probe side partition numbers may be different.
    pub fn new(num_partitions_build_side: usize, num_partitions_probe_side: usize) -> Self {
        NestedLoopJoinOperatorState {
            inner: Mutex::new(SharedOperatorState::Building {
                batches: Vec::new(),
                build_partitions_remaining: num_partitions_build_side,
                probe_side_wakers: vec![None; num_partitions_probe_side],
            }),
        }
    }
}

#[derive(Debug)]
enum SharedOperatorState {
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

        /// Union of all bitmaps across all partitions.
        ///
        /// Referenced with draining unvisited rows in the case of a LEFT join.
        global_outer_join_tracker: Option<LeftOuterJoinTracker>,
    },
}

impl SharedOperatorState {
    /// Transitions the state from `Building` to `Probing`.
    ///
    /// Must be called when number of partitions remaining on the build side is
    /// zero.
    fn transition_into_probing(&mut self, join_type: JoinType) {
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

                let global_outer_join_tracker = match join_type {
                    JoinType::Left | JoinType::Full => {
                        Some(LeftOuterJoinTracker::new_for_batches(batches))
                    }
                    _ => None,
                };

                let batches = std::mem::take(batches);
                *self = Self::Probing {
                    batches: Arc::new(batches),
                    global_outer_join_tracker,
                }
            }
            Self::Probing { .. } => panic!("inner state is already probing"),
        }
    }
}

/// Nested loop join.
#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    /// Filter to apply after cross joining batches.
    filter: Option<PhysicalScalarExpression>,
    join_type: JoinType,
}

impl PhysicalNestedLoopJoin {
    pub const BUILD_SIDE_INPUT_INDEX: usize = 0;
    pub const PROBE_SIDE_INPUT_INDEX: usize = 1;

    pub fn new(filter: Option<PhysicalScalarExpression>, join_type: JoinType) -> Self {
        PhysicalNestedLoopJoin { filter, join_type }
    }
}

impl ExecutableOperator for PhysicalNestedLoopJoin {
    fn create_states2(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        // TODO: Allow different number of partitions on left & right?
        let num_partitions = partitions[0];

        let left_states = (0..num_partitions)
            .map(|_| {
                PartitionState::NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState::default())
            })
            .collect();

        let right_states = (0..num_partitions)
            .map(|partition| {
                PartitionState::NestedLoopJoinProbe(
                    NestedLoopJoinProbePartitionState::new_for_partition(partition),
                )
            })
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::NestedLoopJoin(
                NestedLoopJoinOperatorState::new(num_partitions, num_partitions),
            )),
            partition_states: InputOutputStates::NaryInputSingleOutput {
                partition_states: vec![left_states, right_states],
                pull_states: 1,
            },
        })
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::NestedLoopJoinBuild(state) => {
                state.batches.push(batch);
                Ok(PollPush::Pushed)
            }
            PartitionState::NestedLoopJoinProbe(state) => {
                // Check that the partition-local state has a reference to the
                // global vec of batches.
                if !state.is_populated {
                    // Need to get the global reference.
                    let operator_state = match operator_state {
                        OperatorState::NestedLoopJoin(operater_state) => operater_state,
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    let mut inner = operator_state.inner.lock();
                    match &mut *inner {
                        SharedOperatorState::Building {
                            probe_side_wakers, ..
                        } => {
                            // Still waiting for the build to finish, register
                            // ourselves for a later wakeup when the build is
                            // complete.
                            probe_side_wakers[state.partition_idx] = Some(cx.waker().clone());
                            return Ok(PollPush::Pending(batch));
                        }
                        SharedOperatorState::Probing {
                            batches,
                            global_outer_join_tracker,
                        } => {
                            // Otherwise the batches are ready for us. Clone the
                            // reference into our local state.
                            state.all_batches = batches.clone();
                            state.is_populated = true;

                            if global_outer_join_tracker.is_some() {
                                state.partition_outer_join_tracker =
                                    Some(LeftOuterJoinTracker::new_for_batches(batches))
                            }

                            // Continue...
                        }
                    }
                }

                // If we still have a buffered batch, reschedule the push until
                // it's empty.
                if !state.buffered.is_empty() {
                    state.push_waker = Some(cx.waker().clone());
                    return Ok(PollPush::Pending(batch));
                }

                // Do the join.
                let mut batches = Vec::new();
                for (left_idx, left) in state.all_batches.iter().enumerate() {
                    let mut out = cross_join(
                        left_idx,
                        left,
                        &batch,
                        self.filter.as_ref(),
                        state.partition_outer_join_tracker.as_mut(),
                        false,
                    )?;
                    batches.append(&mut out);
                }

                state.buffered = ComputedBatches::new(batches);
                if state.buffered.is_empty() {
                    // Nothing produces, signal to push more.
                    return Ok(PollPush::NeedsMore);
                }

                // We have stuff in the buffer, wake up the puller.
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollPush::Pushed)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::NestedLoopJoinBuild(state) => {
                let operator_state = match operator_state {
                    OperatorState::NestedLoopJoin(operater_state) => operater_state,
                    other => panic!("invalid operator state: {other:?}"),
                };

                let mut inner = operator_state.inner.lock();
                match &mut *inner {
                    SharedOperatorState::Building {
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
                            inner.transition_into_probing(self.join_type);
                        }

                        // And we're done.
                        Ok(PollFinalize::Finalized)
                    }
                    other => panic!("inner join state is not building: {other:?}"),
                }
            }
            PartitionState::NestedLoopJoinProbe(state) => {
                state.input_finished = true;
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }
                Ok(PollFinalize::Finalized)
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
            PartitionState::NestedLoopJoinProbe(state) => {
                let computed = state.buffered.take();
                if computed.has_batches() {
                    Ok(PollPull::Computed(computed))
                } else if state.input_finished {
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
            PartitionState::NestedLoopJoinBuild(_) => {
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
#[allow(deprecated)]
fn cross_join(
    left_batch_idx: usize,
    left: &Batch,
    right: &Batch,
    filter_expr: Option<&PhysicalScalarExpression>,
    mut left_outer_tracker: Option<&mut LeftOuterJoinTracker>,
    _right_join: bool,
) -> Result<Vec<Batch>> {
    let mut batches = Vec::with_capacity(left.num_rows() * right.num_rows());

    // For each row in the left batch, join the entirety of right.
    for left_idx in 0..left.num_rows() {
        // Create constant selection vector pointing to the one row on the left.
        let selection = SelectionVector::repeated(right.num_rows(), left_idx);

        // Columns from the left, one row repeated.
        let left_columns = left.select_old(Arc::new(selection)).into_arrays();
        // Columns from the right, all rows.
        let right_columns = right.clone().into_arrays();

        let mut output = Batch::try_from_arrays(left_columns.into_iter().chain(right_columns))?;

        // If we have a filter, apply it to the output batch.
        if let Some(filter_expr) = &filter_expr {
            let selection = Arc::new(filter_expr.select(&output)?);
            output = output.select_old(selection.clone());

            // If we're left joining, compute indices in the left batch that we
            // visited.
            if let Some(left_outer_tracker) = &mut left_outer_tracker {
                left_outer_tracker
                    .mark_rows_visited_for_batch(left_batch_idx, selection.iter_locations());
            }
        }

        batches.push(output);
    }

    Ok(batches)
}

impl Explainable for PhysicalNestedLoopJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("NestedLoopJoin").with_value("join_type", self.join_type);

        if let Some(filter) = self.filter.as_ref() {
            ent = ent.with_value("filter", filter);
        }

        ent
    }
}

impl DatabaseProtoConv for PhysicalNestedLoopJoin {
    type ProtoType = rayexec_proto::generated::execution::PhysicalNestedLoopJoin;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     filter: self
        //         .filter
        //         .as_ref()
        //         .map(|f| f.to_proto_ctx(context))
        //         .transpose()?,
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     filter: proto
        //         .filter
        //         .map(|f| PhysicalScalarExpression::from_proto_ctx(f, context))
        //         .transpose()?,
        // })
    }
}
