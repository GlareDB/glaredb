use std::sync::Arc;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_bullet::batch::BatchOld;
use rayexec_error::Result;

use super::util::merger::{KWayMerger, MergeResult};
use super::util::sort_keys::SortKeysExtractor;
use super::util::sorted_batch::{PhysicallySortedBatch, SortedKeysIter};
use crate::database::DatabaseContext;
use crate::execution::operators::sort::util::merger::IterState;
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
use crate::expr::physical::PhysicalSortExpression;
use crate::proto::DatabaseProtoConv;

pub enum MergeSortedPartitionState {
    Pushing {
        partition_idx: usize,
        extractor: SortKeysExtractor,
    },
    Pulling {},
}

/// Partition state on the push side.
#[derive(Debug)]
pub struct GatherSortPushPartitionState {
    /// Index of this partition. Used to emplace buffered batches into the
    /// global state.
    partition_idx: usize,

    /// Extract the sort keys from a batch.
    extractor: SortKeysExtractor,
}

/// Partition state on the pull side.
#[derive(Debug)]
pub struct GatherSortPullPartitionState {
    /// Partition-local buffers for input batches.
    ///
    /// To avoid taking the global lock too frequently, we try to copy in as
    /// many batches as possible into the local state every time we look at the
    /// global state.
    input_buffers: InputBuffers,

    merge_state: PullMergeState,
}

#[derive(Debug)]
struct InputBuffers {
    /// Buffered batches that we retrieve from the global state to avoid needing
    /// to acquire the global lock.
    ///
    /// Indexed by input partition idx.
    buffered: Vec<Option<PhysicallySortedBatch>>,

    /// If each input is finished.
    ///
    /// Indexed by input partition idx.
    finished: Vec<bool>,
}

#[derive(Debug)]
enum PullMergeState {
    /// Currently initialing the state.
    ///
    /// We need at least one batch (or finished==true) from each input input
    /// partition before being able to produce output batches.
    Initializing,

    /// Able to start producing output.
    Producing {
        /// Partitions index for an input that's required. Merging will not
        /// continue until we have a batch from this input.
        input_required: Option<usize>,

        /// Merger for input batches.
        merger: KWayMerger<SortedKeysIter>,
    },
}

#[derive(Debug)]
pub struct GatherSortOperatorState {
    shared: Mutex<SharedGlobalState>,
}

#[derive(Debug)]
struct SharedGlobalState {
    /// Batches from the input partitions.
    ///
    /// Indexed by input partition_idx.
    batches: Vec<Option<PhysicallySortedBatch>>,

    /// If input partitions are finished.
    ///
    /// Indexed by input partition_idx.
    finished: Vec<bool>,

    /// Wakers on the push side.
    ///
    /// If the input partition already has batch in the global shared state,
    /// it'll be marked pending.
    ///
    /// Indexed by input partition_idx.
    push_wakers: Vec<Option<Waker>>,

    /// Waker from the pull side if it doesn't have at least one batch from each
    /// input.
    ///
    /// Paired with the index of the input partition that the pull side is
    /// waiting for.
    ///
    /// Waken only when the specified input partition is able to place a batch
    /// into the global state (or finishes).
    pull_waker: (usize, Option<Waker>),
}

impl SharedGlobalState {
    fn new(num_partitions: usize) -> Self {
        let batches: Vec<_> = (0..num_partitions).map(|_| None).collect();
        let finished: Vec<_> = (0..num_partitions).map(|_| false).collect();
        let push_wakers: Vec<_> = (0..num_partitions).map(|_| None).collect();

        SharedGlobalState {
            batches,
            finished,
            push_wakers,
            pull_waker: (0, None),
        }
    }
}

/// Merge sorted partitions into a single output partition.
#[derive(Debug)]
pub struct PhysicalGatherSort {
    exprs: Vec<PhysicalSortExpression>,
}

impl PhysicalGatherSort {
    pub fn new(exprs: Vec<PhysicalSortExpression>) -> Self {
        PhysicalGatherSort { exprs }
    }

    // TODO: REMOVE
    pub fn create_states_orig(
        &self,
        input_partitions: usize,
    ) -> (
        GatherSortOperatorState,
        Vec<GatherSortPushPartitionState>,
        Vec<GatherSortPullPartitionState>,
    ) {
        let operator_state = GatherSortOperatorState {
            shared: Mutex::new(SharedGlobalState::new(input_partitions)),
        };

        let extractor = SortKeysExtractor::new(&self.exprs);

        let push_states: Vec<_> = (0..input_partitions)
            .map(|idx| GatherSortPushPartitionState {
                partition_idx: idx,
                extractor: extractor.clone(),
            })
            .collect();

        // Note vec with a single element representing a single output
        // partition.
        //
        // I'm not sure if we care to support multiple output partitions, but
        // extending this a little could provide an interesting repartitioning
        // scheme where we repartition based on the sort key.
        let pull_states = vec![GatherSortPullPartitionState {
            input_buffers: InputBuffers {
                buffered: (0..input_partitions).map(|_| None).collect(),
                finished: (0..input_partitions).map(|_| false).collect(),
            },
            merge_state: PullMergeState::Initializing,
        }];

        (operator_state, push_states, pull_states)
    }
}

impl ExecutableOperator for PhysicalGatherSort {
    fn create_states_old(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let input_partitions = partitions[0];

        let operator_state = OperatorState::GatherSort(GatherSortOperatorState {
            shared: Mutex::new(SharedGlobalState::new(input_partitions)),
        });

        let extractor = SortKeysExtractor::new(&self.exprs);

        let push_states: Vec<_> = (0..input_partitions)
            .map(|idx| {
                PartitionState::GatherSortPush(GatherSortPushPartitionState {
                    partition_idx: idx,
                    extractor: extractor.clone(),
                })
            })
            .collect();

        // Note vec with a single element representing a single output
        // partition.
        //
        // I'm not sure if we care to support multiple output partitions, but
        // extending this a little could provide an interesting repartitioning
        // scheme where we repartition based on the sort key.
        let pull_states = vec![PartitionState::GatherSortPull(
            GatherSortPullPartitionState {
                input_buffers: InputBuffers {
                    buffered: (0..input_partitions).map(|_| None).collect(),
                    finished: (0..input_partitions).map(|_| false).collect(),
                },
                merge_state: PullMergeState::Initializing,
            },
        )];

        Ok(ExecutionStates {
            operator_state: Arc::new(operator_state),
            partition_states: InputOutputStates::SeparateInputOutput {
                push_states,
                pull_states,
            },
        })
    }

    fn poll_push_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: BatchOld,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::GatherSortPush(state) => state,
            PartitionState::GatherSortPull(_) => {
                panic!("uses pull state when push state expected")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        let mut shared = match operator_state {
            OperatorState::GatherSort(state) => state.shared.lock(),
            other => panic!("invalid operator state: {other:?}"),
        };

        if shared.batches[state.partition_idx].is_some() {
            // Can't push, global state already has a batch for this partition.
            shared.push_wakers[state.partition_idx] = Some(cx.waker().clone());
            return Ok(PollPush::Pending(batch));
        }

        let keys = state.extractor.sort_keys(&batch)?;
        let sorted = PhysicallySortedBatch { batch, keys };
        shared.batches[state.partition_idx] = Some(sorted);

        // Wake up the pull side if its waiting on this partition.
        if shared.pull_waker.0 == state.partition_idx {
            if let Some(waker) = shared.pull_waker.1.take() {
                waker.wake();
            }
        }

        // TODO: Might change this to NeedsMore for clarity. This operator is
        // always the "sink" for a pipeline, and so every time this is reached,
        // the pipeline starts to execute from the beginning to try to get more
        // batches into the sink. NeedsMore essentially does the same thing, no
        // matter where the operator is in the pipeline.
        //
        // Changing this to NeedsMore wouldn't change behavior.
        Ok(PollPush::Pushed)
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::GatherSortPush(state) => state,
            PartitionState::GatherSortPull(_) => {
                panic!("uses pull state when push state expected")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        let mut shared = match operator_state {
            OperatorState::GatherSort(state) => state.shared.lock(),
            other => panic!("invalid operator state: {other:?}"),
        };

        shared.finished[state.partition_idx] = true;

        // Wake up the pull side if its waiting on this partition.
        if shared.pull_waker.0 == state.partition_idx {
            if let Some(waker) = shared.pull_waker.1.take() {
                waker.wake();
            }
        }

        Ok(PollFinalize::Finalized)
    }

    fn poll_pull_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::GatherSortPull(state) => state,
            PartitionState::GatherSortPush(_) => {
                panic!("uses push state when pull state expected")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::GatherSort(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        // Finish up initialization if needed.
        if let PullMergeState::Initializing = &state.merge_state {
            match Self::try_finish_initialize(cx, &mut state.input_buffers, operator_state)? {
                Some(merger) => {
                    // Flip state and continue.
                    state.merge_state = PullMergeState::Producing {
                        input_required: None,
                        merger,
                    }
                }
                None => {
                    // Not finished initializing, still waiting on some input.
                    //
                    // `try_finish_initialize` registers a waker for us.
                    return Ok(PollPull::Pending);
                }
            }
        }

        match &mut state.merge_state {
            PullMergeState::Producing {
                input_required,
                merger,
            } => {
                if let Some(input_idx) = input_required {
                    let input_pushed = Self::try_push_input_batch_to_merger(
                        cx,
                        merger,
                        &mut state.input_buffers,
                        operator_state,
                        *input_idx,
                    )?;
                    if !input_pushed {
                        // `try_push_input_batch_to_merger` registers a waker for us.
                        return Ok(PollPull::Pending);
                    }

                    // Input no longer required, we've either pushed the batch
                    // to the merger, or let the merger know that that input's
                    // finished.
                    *input_required = None;
                }

                // Now try to merge.
                //
                // We loop to try to make as much progress with the merger using
                // our local buffered batches as much as possible.
                loop {
                    // TODO: Configurable batch size.
                    match merger.try_merge(1024)? {
                        MergeResult::Batch(batch) => return Ok(PollPull::Computed(batch.into())),
                        MergeResult::NeedsInput(input_idx) => {
                            let pushed = Self::try_push_input_batch_to_merger(
                                cx,
                                merger,
                                &mut state.input_buffers,
                                operator_state,
                                input_idx,
                            )?;

                            if pushed {
                                // Keep trying to merge
                                continue;
                            } else {
                                // Batch not available yet. Waker
                                // registered by
                                // `try_push_input_batch_to_merger`.
                                //
                                // Store input that we need to try to
                                // get a batch from so that the next
                                // call to `poll_pull` ensures that we
                                // get that input.
                                *input_required = Some(input_idx);
                                return Ok(PollPull::Pending);
                            }
                        }
                        MergeResult::Exhausted => return Ok(PollPull::Exhausted),
                    }
                }
            }
            PullMergeState::Initializing => unreachable!("should should be 'producing' by now"),
        }
    }
}

impl PhysicalGatherSort {
    /// Try to finish the pull-side initialization step.
    ///
    /// This will try to get a batch from each input partition so we can
    /// initialize the merger. If we are able to initialize the merger, the
    /// partition's merge state is flipped to Producing.
    ///
    /// Returns the initialized merger on success.
    ///
    /// If this returns None, our waker will be registered in the global state
    /// for the input partition we're waiting on.
    fn try_finish_initialize(
        cx: &mut Context,
        input_buffers: &mut InputBuffers,
        operator_state: &GatherSortOperatorState,
    ) -> Result<Option<KWayMerger<SortedKeysIter>>> {
        let mut shared = operator_state.shared.lock();
        for (idx, local_buf) in input_buffers.buffered.iter_mut().enumerate() {
            if local_buf.is_none() {
                if let Some(batch) = shared.batches[idx].take() {
                    *local_buf = Some(batch);
                    // Global state has room for another batch, wake a pending
                    // waker to try to get more.
                    if let Some(waker) = shared.push_wakers[idx].take() {
                        waker.wake();
                    }
                }
            }
            input_buffers.finished[idx] = shared.finished[idx];
        }

        // Find the partition index that we still need input for.
        let need_partition = input_buffers
            .buffered
            .iter()
            .zip(input_buffers.finished.iter())
            .position(|(batch, finished)| batch.is_none() && !finished);

        match need_partition {
            Some(partition_idx) => {
                // Need to wait for input from this partition.
                shared.pull_waker = (partition_idx, Some(cx.waker().clone()));
                Ok(None)
            }
            None => {
                // Otherwise we can begin merging.
                std::mem::drop(shared); // No need to keep the lock.

                let mut inputs = Vec::with_capacity(input_buffers.buffered.len());
                for (batch, finished) in input_buffers
                    .buffered
                    .iter_mut()
                    .zip(input_buffers.finished.iter())
                {
                    match batch.take() {
                        Some(batch) => {
                            let (batch, iter) = batch.into_batch_and_iter();
                            inputs.push((Some(batch), IterState::Iterator(iter)));
                        }
                        None => {
                            assert!(
                                finished,
                                "partition input must be finished if no batches produced"
                            );
                            inputs.push((None, IterState::Finished));
                        }
                    }
                }
                assert_eq!(inputs.len(), input_buffers.buffered.len());

                let merger = KWayMerger::try_new(inputs)?;
                // We're good, state's been flipped and we can continue with the
                // pull.
                Ok(Some(merger))
            }
        }
    }

    /// Try to push an batch for input indicated by `input_idx` to the merger.
    ///
    /// This will first check to see if we have a batch ready in the
    /// partition-local state. If not, we'll then check the global state.
    ///
    /// If we are able to get a batch for the input, or we see that the input is
    /// finished, the merger will be updated with that info. Otherwise, we
    /// register our waker in the global state, and we need to wait.
    ///
    /// Returns true on successfully getting a batch (or seeing that
    /// finished==true). Returns false otherwise.
    fn try_push_input_batch_to_merger(
        cx: &mut Context,
        merger: &mut KWayMerger<SortedKeysIter>,
        input_buffers: &mut InputBuffers,
        operator_state: &GatherSortOperatorState,
        input_idx: usize,
    ) -> Result<bool> {
        // We need to make sure we have a batch from this input. Try from the
        // partition state first, then look in the global state.
        match input_buffers.buffered[input_idx].take() {
            Some(batch) => {
                // We're good, go ahead and given this batch to the
                // merger and keep going.
                let (batch, iter) = batch.into_batch_and_iter();
                merger.push_batch_for_input(input_idx, batch, iter)?;

                Ok(true)
            }
            None => {
                // Check if this input is finished, and let the
                // merger know if so.
                if input_buffers.finished[input_idx] {
                    merger.input_finished(input_idx);
                    Ok(true)
                } else {
                    // Otherwise need to get from global input_buffers.
                    let mut shared = operator_state.shared.lock();

                    // Copy in as many batches as we can from the
                    // global input_buffers.
                    for (idx, local_buf) in input_buffers.buffered.iter_mut().enumerate() {
                        if local_buf.is_none() {
                            *local_buf = shared.batches[idx].take();
                        }
                        input_buffers.finished[idx] = shared.finished[idx];
                    }

                    // Check local input_buffers again.
                    match (
                        input_buffers.buffered[input_idx].take(),
                        input_buffers.finished[input_idx],
                    ) {
                        (Some(batch), _) => {
                            // We have a batch read to go, give it
                            // to the merge and continue.
                            let (batch, iter) = batch.into_batch_and_iter();
                            merger.push_batch_for_input(input_idx, batch, iter)?;
                            Ok(true)
                        }
                        (None, true) => {
                            // Input is finished, let the merger
                            // know and continue.
                            merger.input_finished(input_idx);
                            Ok(true)
                        }
                        (None, false) => {
                            // Need to wait for a batch, register
                            // our waker, and return Pending.
                            shared.pull_waker = (input_idx, Some(cx.waker().clone()));
                            Ok(false)
                        }
                    }
                }
            }
        }
    }
}

impl Explainable for PhysicalGatherSort {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("GatherSort")
    }
}

impl DatabaseProtoConv for PhysicalGatherSort {
    type ProtoType = rayexec_proto::generated::execution::PhysicalMergeSortedInputs;

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
    use std::sync::Arc;

    use super::*;
    use crate::execution::operators::test_util::{
        make_i32_batch,
        unwrap_poll_pull_batch,
        TestWakerContext,
    };
    use crate::expr::physical::column_expr::PhysicalColumnExpr;

    #[test]
    fn merge_sorted_single_input_partition() {
        let mut p0_inputs = vec![
            make_i32_batch([8, 6, 6]),
            make_i32_batch([5, 4, 3, 2]),
            make_i32_batch([1, 1, 1]),
        ];

        let operator = Arc::new(PhysicalGatherSort::new(vec![PhysicalSortExpression {
            column: PhysicalColumnExpr { idx: 0 },
            desc: true,
            nulls_first: true,
        }]));
        let (operator_state, push_states, pull_states) = operator.create_states_orig(1);
        let operator_state = Arc::new(OperatorState::GatherSort(operator_state));
        let mut push_states: Vec<_> = push_states
            .into_iter()
            .map(PartitionState::GatherSortPush)
            .collect();
        let mut pull_states: Vec<_> = pull_states
            .into_iter()
            .map(PartitionState::GatherSortPull)
            .collect();

        // Try to pull first. Nothing available yet.
        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Pending, poll_pull);

        // Push our first batch.
        let push_cx = TestWakerContext::new();
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut push_states[0],
                &operator_state,
                p0_inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Pushed, poll_push);

        // Kind of an implementation detail, but the puller is waiting on
        // partition 0 to push. Multiple partitions would trigger this wakeup
        // too even though the other partitions might not be ready yet.
        //
        // It's possible for the implementation to change to only have the wake
        // up happen if all partitions are ready, but that's an unknown
        // complexity right now. We could experiment with it.
        assert_eq!(1, pull_cx.wake_count());

        // Push the rest of our batches. Note that we need to interleave pulls
        // and pushes because the global state has a buffer for only one batch.
        // The pull will get it from the the global state and push it into the
        // merger, free up space.
        for p1_input in p0_inputs {
            let poll_pull = pull_cx
                .poll_pull(&operator, &mut pull_states[0], &operator_state)
                .unwrap();
            assert_eq!(PollPull::Pending, poll_pull);

            let poll_push = push_cx
                .poll_push(&operator, &mut push_states[0], &operator_state, p1_input)
                .unwrap();
            assert_eq!(PollPush::Pushed, poll_push);
        }

        // Partition input is finished.
        operator
            .poll_finalize_push_old(&mut push_cx.context(), &mut push_states[0], &operator_state)
            .unwrap();

        // Now we can pull the sorted result.
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([8, 6, 6, 5, 4, 3, 2, 1, 1, 1]);
        assert_eq!(expected, output);

        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Exhausted, poll_pull);
    }

    #[test]
    fn merge_sorted_two_input_partitions() {
        let mut p0_inputs = vec![
            make_i32_batch([8, 6, 6]),
            make_i32_batch([5, 4, 3, 2]),
            make_i32_batch([1, 1, 1]),
        ];
        let mut p1_inputs = vec![
            make_i32_batch([10, 10, 9]),
            make_i32_batch([7, 4, 0]),
            make_i32_batch([-1, -2]),
        ];

        let operator = Arc::new(PhysicalGatherSort::new(vec![PhysicalSortExpression {
            column: PhysicalColumnExpr { idx: 0 },
            desc: true,
            nulls_first: true,
        }]));
        let (operator_state, push_states, pull_states) = operator.create_states_orig(2);
        let operator_state = Arc::new(OperatorState::GatherSort(operator_state));
        let mut push_states: Vec<_> = push_states
            .into_iter()
            .map(PartitionState::GatherSortPush)
            .collect();
        let mut pull_states: Vec<_> = pull_states
            .into_iter()
            .map(PartitionState::GatherSortPull)
            .collect();

        // Pull first, get pending
        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Pending, poll_pull);

        // Push batch for partition 0.
        let p0_push_cx = TestWakerContext::new();
        let poll_push = p0_push_cx
            .poll_push(
                &operator,
                &mut push_states[0],
                &operator_state,
                p0_inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Pushed, poll_push);

        // Triggers pull wake up.
        assert_eq!(1, pull_cx.wake_count());

        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Pending, poll_pull);

        // Push batch for partition 1.
        let p1_push_cx = TestWakerContext::new();
        let poll_push = p1_push_cx
            .poll_push(
                &operator,
                &mut push_states[1],
                &operator_state,
                p1_inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Pushed, poll_push);

        // Also triggers wake up.
        assert_eq!(1, pull_cx.wake_count());

        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Pending, poll_pull);

        // Push the rest of the batches.
        //
        // As above, we go back and forth between pushing and pulling to ensure
        // the global state buffer is free for each input partition.
        for (p0_input, p1_input) in p0_inputs.into_iter().zip(p1_inputs.into_iter()) {
            let poll_pull = pull_cx
                .poll_pull(&operator, &mut pull_states[0], &operator_state)
                .unwrap();
            assert_eq!(PollPull::Pending, poll_pull);

            let poll_push = p0_push_cx
                .poll_push(&operator, &mut push_states[0], &operator_state, p0_input)
                .unwrap();
            assert_eq!(PollPush::Pushed, poll_push);

            let poll_push = p1_push_cx
                .poll_push(&operator, &mut push_states[1], &operator_state, p1_input)
                .unwrap();
            assert_eq!(PollPush::Pushed, poll_push);
        }

        // Partition inputs is finished.
        operator
            .poll_finalize_push_old(
                &mut p0_push_cx.context(),
                &mut push_states[0],
                &operator_state,
            )
            .unwrap();
        operator
            .poll_finalize_push_old(
                &mut p1_push_cx.context(),
                &mut push_states[1],
                &operator_state,
            )
            .unwrap();

        // Now we can pull the sorted result.
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([10, 10, 9, 8, 7, 6, 6, 5, 4, 4, 3, 2, 1, 1, 1, 0, -1, -2]);
        assert_eq!(expected, output);

        let poll_pull = pull_cx
            .poll_pull(&operator, &mut pull_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Exhausted, poll_pull);
    }
}
