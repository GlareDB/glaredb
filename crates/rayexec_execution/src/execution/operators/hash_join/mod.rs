pub mod condition;
pub mod table;

use condition::HashJoinCondition;
use parking_lot::Mutex;
use rayexec_bullet::{batch::Batch, compute, datatype::DataType};
use rayexec_error::{RayexecError, Result};
use std::{
    sync::Arc,
    task::{Context, Waker},
};
use table::JoinHashTable;

use crate::{
    database::DatabaseContext,
    execution::operators::util::hash::{AhashHasher, ArrayHasher},
    explain::explainable::{ExplainConfig, ExplainEntry, Explainable},
    logical::logical_join::JoinType,
};

use super::{
    util::outer_join_tracker::{LeftOuterJoinDrainState, LeftOuterJoinTracker},
    ExecutableOperator, ExecutionStates, InputOutputStates, OperatorState, PartitionState,
    PollFinalize, PollPull, PollPush,
};

#[derive(Debug)]
pub struct HashJoinBuildPartitionState {
    /// Hash table this partition will be writing to.
    local_hashtable: JoinHashTable,
    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
}

#[derive(Debug)]
pub struct HashJoinProbePartitionState {
    /// Index of this partition.
    partition_idx: usize,
    /// The final output table. If None, the global state should be checked to
    /// see if it's ready to copy into the partition local state.
    global: Option<Arc<JoinHashTable>>,
    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
    /// Buffered output batch.
    buffered_output: Option<Batch>,
    /// Waker that's stored from a push if there's already a buffered batch.
    push_waker: Option<Waker>,
    /// Waker that's stored from a pull if there's no batch available.
    pull_waker: Option<Waker>,
    /// If the input for this partiton is complete.
    input_finished: bool,
    /// Track rows visited on the left side for this partition.
    ///
    /// Initalized after build completes.
    partition_outer_join_tracker: Option<LeftOuterJoinTracker>,
    /// State for tracking rows on the left side that we still need to emit.
    ///
    /// This is currently populated for one partition at the end of probing.
    outer_join_drain_state: Option<LeftOuterJoinDrainState>,
}

#[derive(Debug)]
pub struct HashJoinOperatorState {
    inner: Mutex<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    /// The partially built global hash table.
    ///
    /// Input partitions merge their partition-local hash table into this global
    /// table once they complete.
    partial: JoinHashTable,
    /// Number of build inputs remaining.
    ///
    /// Initially set to number of build partitions.
    build_inputs_remaining: usize,
    /// Number of probe inputs remaining.
    ///
    /// Initially set to number of probe partitions.
    probe_inputs_remaining: usize,
    /// The shared global hash table once it's been fully built.
    ///
    /// This is None if there's still inputs still building.
    shared_global: Option<Arc<JoinHashTable>>,
    /// Union of all bitmaps across all partitions.
    ///
    /// Referenced with draining unvisited rows in the case of a LEFT join.
    ///
    /// Initialized when build completes.
    global_outer_join_tracker: Option<LeftOuterJoinTracker>,
    /// Pending wakers for thread that attempted to probe the table prior to it
    /// being built.
    ///
    /// Indexed by probe partition index.
    ///
    /// Woken once the global hash table has been completed (moved into
    /// `shared_global`).
    probe_push_wakers: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    join_type: JoinType,
    equality: HashJoinCondition,
    conditions: Vec<HashJoinCondition>,
    /// Types for the batches we'll be receiving from the left side. Used during
    /// RIGHT joins to produce null columns on the left side.
    left_types: Vec<DataType>,
    /// Types for the batches we'll be receiving from the right side. Used
    /// during LEFT joins to produce null columns on the right side.
    right_types: Vec<DataType>,
}

impl PhysicalHashJoin {
    pub const BUILD_SIDE_INPUT_INDEX: usize = 0;
    pub const PROBE_SIDE_INPUT_INDEX: usize = 1;

    /// Try to create a new hash join operator.
    ///
    /// `equality_idx` should point to the equality condition in `conditions`.
    pub fn new(
        join_type: JoinType,
        equality_idx: usize,
        conditions: Vec<HashJoinCondition>,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
    ) -> Self {
        let equality = conditions[equality_idx].clone();

        PhysicalHashJoin {
            join_type,
            equality,
            conditions,
            left_types,
            right_types,
        }
    }

    const fn is_left_join(&self) -> bool {
        matches!(self.join_type, JoinType::Left | JoinType::Full)
    }
}

impl ExecutableOperator for PhysicalHashJoin {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        // TODO: Determine if this is what we want.
        let build_partitions = partitions[0];
        let probe_partitions = partitions[0];

        let right_join = matches!(self.join_type, JoinType::Full | JoinType::Right);

        let shared = SharedState {
            partial: JoinHashTable::new(self.left_types.clone(), &self.conditions, right_join),
            build_inputs_remaining: build_partitions,
            probe_inputs_remaining: probe_partitions,
            shared_global: None,
            global_outer_join_tracker: None,
            probe_push_wakers: vec![None; probe_partitions],
        };

        let operator_state = HashJoinOperatorState {
            inner: Mutex::new(shared),
        };

        let build_states: Vec<_> = (0..build_partitions)
            .map(|_| {
                PartitionState::HashJoinBuild(HashJoinBuildPartitionState {
                    local_hashtable: JoinHashTable::new(
                        self.left_types.clone(),
                        &self.conditions,
                        right_join,
                    ),
                    hash_buf: Vec::new(),
                })
            })
            .collect();

        let probe_states: Vec<_> = (0..probe_partitions)
            .map(|idx| {
                PartitionState::HashJoinProbe(HashJoinProbePartitionState {
                    partition_idx: idx,
                    global: None,
                    hash_buf: Vec::new(),
                    buffered_output: None,
                    push_waker: None,
                    pull_waker: None,
                    input_finished: false,
                    partition_outer_join_tracker: None,
                    outer_join_drain_state: None,
                })
            })
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::HashJoin(operator_state)),
            partition_states: InputOutputStates::NaryInputSingleOutput {
                partition_states: vec![build_states, probe_states],
                pull_states: Self::PROBE_SIDE_INPUT_INDEX,
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
            PartitionState::HashJoinBuild(state) => {
                // Compute left hashes on equality condition.
                let result = self.equality.left.eval(&batch)?;
                state.hash_buf.clear();
                state.hash_buf.resize(result.len(), 0);
                let hashes = AhashHasher::hash_arrays(&[result.as_ref()], &mut state.hash_buf)?;

                state.local_hashtable.insert_batch(batch, hashes)?;

                Ok(PollPush::Pushed)
            }
            PartitionState::HashJoinProbe(state) => {
                // If we have pending output, we need to wait for that to get
                // pulled before trying to compute additional batches.
                if state.buffered_output.is_some() {
                    state.push_waker = Some(cx.waker().clone());
                    return Ok(PollPush::Pending(batch));
                }

                let operator_state = match operator_state {
                    OperatorState::HashJoin(state) => state,
                    other => panic!("invalid operator state: {other:?}"),
                };

                // Check if we have the final hash table, if not, look in he
                // global state.
                if state.global.is_none() {
                    let mut shared = operator_state.inner.lock();

                    // If there's still some inputs building, just store our
                    // waker to come back later.
                    if shared.build_inputs_remaining != 0 {
                        shared.probe_push_wakers[state.partition_idx] = Some(cx.waker().clone());
                        return Ok(PollPush::Pending(batch));
                    }

                    // Final partition on the build side should be what sets
                    // this. So if build_inputs_remaining == 0, then it should
                    // exist.
                    let shared_global = shared
                        .shared_global
                        .clone()
                        .expect("shared global table should exist, no inputs remaining");

                    // Create initial visit bitmaps that will be tracked by this
                    // partition.
                    if self.is_left_join() {
                        state.partition_outer_join_tracker =
                            Some(LeftOuterJoinTracker::new_for_batches(
                                shared_global.collected_batches(),
                            ));
                    }

                    // Final hash table built, store in our partition local
                    // state.
                    state.global = Some(shared_global);

                    // Continue on.
                }

                // Compute right hashes on equality condition.
                let result = self.equality.right.eval(&batch)?;
                state.hash_buf.clear();
                state.hash_buf.resize(result.len(), 0);
                let hashes = AhashHasher::hash_arrays(&[result.as_ref()], &mut state.hash_buf)?;

                let hashtable = state.global.as_ref().expect("hash table to exist");

                let batches =
                    hashtable.probe(&batch, hashes, state.partition_outer_join_tracker.as_mut())?;

                if batches.is_empty() {
                    // No batches joined, keep pushing to this operator.
                    return Ok(PollPush::NeedsMore);
                }

                // TODO: Would be cool not having to do this.
                let batch = compute::concat::concat_batches(&batches)?;
                state.buffered_output = Some(batch);

                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollPush::Pushed)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let mut shared = match operator_state {
            OperatorState::HashJoin(state) => state.inner.lock(),
            other => panic!("invalid operator state: {other:?}"),
        };

        match partition_state {
            PartitionState::HashJoinBuild(state) => {
                // Merge local table into the global table.
                let local_table = std::mem::replace(
                    &mut state.local_hashtable,
                    JoinHashTable::new(Vec::new(), &[], false), // TODO: A bit hacky.
                );
                shared.partial.merge(local_table)?;

                shared.build_inputs_remaining -= 1;

                // If we're the last remaining, go ahead and move the 'partial'
                // table to 'global', and wake up any pending probers.
                //
                // Probers will then clone the global hash table (behind an Arc)
                // into their local states to avoid needing to synchronize.
                if shared.build_inputs_remaining == 0 {
                    let global_table = std::mem::replace(
                        &mut shared.partial,
                        JoinHashTable::new(Vec::new(), &[], false), // TODO: A bit hacky.
                    );

                    // Init global left tracker too if needed.
                    if self.is_left_join() {
                        shared.global_outer_join_tracker = Some(
                            LeftOuterJoinTracker::new_for_batches(global_table.collected_batches()),
                        )
                    }

                    shared.shared_global = Some(Arc::new(global_table));

                    for waker in shared.probe_push_wakers.iter_mut() {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }
                }

                Ok(PollFinalize::Finalized)
            }
            PartitionState::HashJoinProbe(state) => {
                // Ensure we've finished building the left side before
                // continuing with the finalize.
                //
                // This is important for left joins since we need to flush out
                // unvisited rows which we can only do once we have the complete
                // left side.
                if shared.build_inputs_remaining != 0 {
                    shared.probe_push_wakers[state.partition_idx] = Some(cx.waker().clone());
                    return Ok(PollFinalize::Pending);
                }

                state.input_finished = true;

                // Set partition-local global hash table reference if we don't
                // have it. It's possible for this partition not have this if we
                // pushed no batches for this partition.
                //
                // We want to ensure this is set no matter the join type.
                if state.global.is_none() {
                    state.global = shared.shared_global.clone();
                }

                shared.probe_inputs_remaining -= 1;

                if self.is_left_join() {
                    let probe_finished = shared.probe_inputs_remaining == 0;

                    // Merge local left visit bitmaps into global if we have it.
                    let global = match shared.global_outer_join_tracker.as_mut() {
                        Some(global) => global,
                        None => {
                            return Err(RayexecError::new(
                                "Global left outer tracker unexpectedly None",
                            ))
                        }
                    };

                    // Local may be none if this partition didn't receive any
                    // batches for probing. Tracker initialized on first batch
                    // we probe with.
                    if let Some(local) = &state.partition_outer_join_tracker {
                        global.merge_from(local)
                    }

                    // If we're the last probe partition, set up state to drain all
                    // unvisited rows from left.
                    //
                    // TODO: Allow multiple partitions to drain. Should be
                    // doable by taking hash table and partioning collected
                    // batches back out to probe partitions, and each probe
                    // partition have its own drain state.
                    if probe_finished {
                        state.outer_join_drain_state = Some(LeftOuterJoinDrainState::new(
                            global.clone(),
                            shared
                                .shared_global
                                .as_ref()
                                .unwrap()
                                .collected_batches()
                                .to_vec(),
                            self.left_types.clone(),
                            self.right_types.clone(),
                        ))
                    }
                }

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
        let state = match partition_state {
            PartitionState::HashJoinProbe(state) => state,
            PartitionState::HashJoinBuild(_) => {
                // We should only be pulling with the "probe" state. The "build"
                // state acts as a sink into the operator.
                panic!("should not pull with a build state")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.buffered_output.take() {
            Some(batch) => {
                // Partition has space available, go ahead an wake a pending
                // pusher.
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }

                Ok(PollPull::Batch(batch))
            }
            None => {
                if state.input_finished {
                    // Check if we're still draining unvisited left rows.
                    if let Some(drain_state) = state.outer_join_drain_state.as_mut() {
                        match drain_state.drain_next()? {
                            Some(batch) => return Ok(PollPull::Batch(batch)),
                            None => return Ok(PollPull::Exhausted),
                        }
                    }

                    // We're done.
                    return Ok(PollPull::Exhausted);
                }

                // No batch available, come back later.
                state.pull_waker = Some(cx.waker().clone());

                // Wake up a pusher since there's space available.
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }

                Ok(PollPull::Pending)
            }
        }
    }
}

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashJoin")
            .with_values("conditions", &self.conditions)
            .with_value("equality", &self.equality)
            .with_value("join_type", self.join_type)
    }
}
