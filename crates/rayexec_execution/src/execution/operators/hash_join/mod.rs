pub mod condition;

mod global_hash_table;
mod partition_hash_table;

use std::sync::Arc;
use std::task::{Context, Waker};

use condition::HashJoinCondition;
use global_hash_table::GlobalHashTable;
use parking_lot::Mutex;
use partition_hash_table::PartitionHashTable;
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::scalar::HashExecutor;
use rayexec_error::{OptionExt, RayexecError, Result};

use super::util::outer_join_tracker::{LeftOuterJoinDrainState, LeftOuterJoinTracker};
use super::{
    ComputedBatches,
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct HashJoinBuildPartitionState {
    /// Hash table this partition will be writing to.
    ///
    /// Optional to enable moving from the local to global state once this
    /// partition finishes building.
    local_hashtable: Option<PartitionHashTable>,
    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
}

#[derive(Debug)]
pub struct HashJoinProbePartitionState {
    /// Index of this partition.
    partition_idx: usize,
    /// The final output table. If None, the global state should be checked to
    /// see if it's ready to copy into the partition local state.
    global: Option<Arc<GlobalHashTable>>,
    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
    /// Buffered output batch.
    buffered_output: ComputedBatches,
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
    /// Partition local hash tables that have been completed on the build side.
    ///
    /// Once all partitions have finished building, a single thread will merge
    /// all tables into a single global table for probing.
    completed_hash_tables: Vec<PartitionHashTable>,
    /// Global hash table once it's been built.
    global_hash_table: Option<Arc<GlobalHashTable>>,
    /// Number of partitions that are probiding the table.
    ///
    /// This is used to initialize the drain states such that each partition
    /// processes different batch sets.
    probe_partition_count: usize,
    /// Number of build inputs remaining.
    ///
    /// Initially set to number of build partitions.
    build_inputs_remaining: usize,
    /// Number of probe inputs remaining.
    ///
    /// Initially set to number of probe partitions.
    probe_inputs_remaining: usize,
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
    /// Pending wakers for threads waiting for the global outer join tracker to
    /// have all partition inputs merged into it.
    ///
    /// Indexed by probe partition index.
    probe_drain_wakers: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    join_type: JoinType,
    /// All left/right equalities we'll be checking.
    equalities: Vec<HashJoinCondition>,
    /// All left/right conditions we'll be checking.
    ///
    /// This includes the above equalities.
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
        equality_inidices: &[usize],
        conditions: Vec<HashJoinCondition>,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
    ) -> Self {
        assert!(!equality_inidices.is_empty());

        let equalities = equality_inidices
            .iter()
            .map(|idx| conditions[*idx].clone())
            .collect();

        PhysicalHashJoin {
            join_type,
            equalities,
            conditions,
            left_types,
            right_types,
        }
    }

    const fn join_requires_drain(&self) -> bool {
        // Note that while a SEMI join is pretty much an inner join just with
        // the right chopped off, we need to be able to handle duplicate rows on
        // the left. So we use the same mechanism for the LEFT MARK join to
        // accomplish the deduplication.
        matches!(
            self.join_type,
            JoinType::Left | JoinType::Full | JoinType::Semi | JoinType::LeftMark { .. }
        )
    }

    const fn is_right_join(&self) -> bool {
        matches!(self.join_type, JoinType::Full | JoinType::Right)
    }

    const fn is_mark_join(&self) -> bool {
        // Note this includes SEMI join since it's just an extension of a mark
        // join, just that we return the left visited rows instead of bools that
        // they've been visited.
        matches!(self.join_type, JoinType::Semi | JoinType::LeftMark { .. })
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

        let shared = SharedState {
            completed_hash_tables: Vec::with_capacity(build_partitions),
            global_hash_table: None,
            probe_partition_count: probe_partitions,
            build_inputs_remaining: build_partitions,
            probe_inputs_remaining: probe_partitions,
            global_outer_join_tracker: None,
            probe_push_wakers: vec![None; probe_partitions],
            probe_drain_wakers: vec![None; probe_partitions],
        };

        let operator_state = HashJoinOperatorState {
            inner: Mutex::new(shared),
        };

        let build_states: Vec<_> = (0..build_partitions)
            .map(|_| {
                PartitionState::HashJoinBuild(HashJoinBuildPartitionState {
                    local_hashtable: Some(PartitionHashTable::new(&self.conditions)),
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
                    buffered_output: ComputedBatches::None,
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
        batch: BatchOld,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::HashJoinBuild(state) => {
                self.insert_into_local_table(state, batch)?;
                Ok(PollPush::NeedsMore)
            }
            PartitionState::HashJoinProbe(state) => {
                // If we have pending output, we need to wait for that to get
                // pulled before trying to compute additional batches.
                if !state.buffered_output.is_empty() {
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

                    let global = match shared.global_hash_table.as_ref() {
                        Some(table) => table.clone(),
                        None => {
                            // Table might still be being built up by another
                            // thread. Come back when it's ready.
                            shared.probe_push_wakers[state.partition_idx] =
                                Some(cx.waker().clone());
                            return Ok(PollPush::Pending(batch));
                        }
                    };

                    // Create initial visit bitmaps that will be tracked by this
                    // partition.
                    if self.join_requires_drain() {
                        state.partition_outer_join_tracker = Some(
                            LeftOuterJoinTracker::new_for_batches(global.collected_batches()),
                        );
                    }

                    // Final hash table built, store in our partition local
                    // state.
                    state.global = Some(global);

                    // Continue on.
                }

                // Compute right hashes on equality condition.
                state.hash_buf.clear();
                state.hash_buf.resize(batch.num_rows(), 0);

                for (idx, equality) in self.equalities.iter().enumerate() {
                    let result = equality.right.eval(&batch)?;

                    if idx == 0 {
                        HashExecutor::hash_no_combine(&result, &mut state.hash_buf)?;
                    } else {
                        HashExecutor::hash_combine(&result, &mut state.hash_buf)?;
                    }
                }

                let hashtable = state.global.as_ref().expect("hash table to exist");

                let batches = hashtable.probe(
                    &batch,
                    &state.hash_buf,
                    state.partition_outer_join_tracker.as_mut(),
                )?;

                state.buffered_output = ComputedBatches::new(batches);
                if state.buffered_output.is_empty() {
                    // No batches joined, keep pushing to this operator.
                    return Ok(PollPush::NeedsMore);
                }

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
        match partition_state {
            PartitionState::HashJoinBuild(state) => {
                let mut shared = match operator_state {
                    OperatorState::HashJoin(state) => state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                // Move local table into global state.
                match state.local_hashtable.take() {
                    Some(table) => shared.completed_hash_tables.push(table),
                    None => return Err(RayexecError::new("Missing partition table")), // Shouldn't happen.
                }

                shared.build_inputs_remaining -= 1;

                // If we're the last remaining, this thread will be responsible
                // for building the global hash table and putting it in the
                // global state.
                //
                // Probers will then clone the global hash table (behind an Arc)
                // into their local states to avoid needing to synchronize.
                if shared.build_inputs_remaining == 0 {
                    let completed = std::mem::take(&mut shared.completed_hash_tables);

                    // Release the lock. Building the table can be
                    // computationally expensive. Other threads still need
                    // access to the global state to register wakers.
                    std::mem::drop(shared);

                    let global = GlobalHashTable::new(
                        self.left_types.clone(),
                        self.is_right_join(),
                        self.is_mark_join(),
                        completed,
                        &self.conditions,
                    );

                    // Reacquire, and place in global state.
                    let mut shared = match operator_state {
                        OperatorState::HashJoin(state) => state.inner.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    // Init global left tracker too if needed.
                    if self.join_requires_drain() {
                        shared.global_outer_join_tracker = Some(
                            LeftOuterJoinTracker::new_for_batches(global.collected_batches()),
                        )
                    }

                    shared.global_hash_table = Some(Arc::new(global));

                    // Wake up probers. They can make progress now.
                    for waker in shared.probe_push_wakers.iter_mut() {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }
                }

                Ok(PollFinalize::Finalized)
            }
            PartitionState::HashJoinProbe(state) => {
                let mut shared = match operator_state {
                    OperatorState::HashJoin(state) => state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

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

                // It's possible for this partition not have this if we pushed
                // no batches for this partition. Ensure we have it (no matter
                // the join type).
                match shared.global_hash_table.as_ref() {
                    Some(table) => {
                        if state.global.is_none() {
                            state.global = Some(table.clone());
                        }
                    }
                    None => {
                        shared.probe_push_wakers[state.partition_idx] = Some(cx.waker().clone());
                        return Ok(PollFinalize::Pending);
                    }
                }

                state.input_finished = true;
                shared.probe_inputs_remaining -= 1;

                if self.join_requires_drain() {
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

                    if probe_finished {
                        // Wake up pending probers, they can initialize drain
                        // states now.
                        for waker in shared.probe_drain_wakers.iter_mut() {
                            if let Some(waker) = waker.take() {
                                waker.wake();
                            }
                        }
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
        operator_state: &OperatorState,
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

        let computed = state.buffered_output.take();
        if computed.has_batches() {
            // Partition has space available, go ahead an wake a pending
            // pusher.
            if let Some(waker) = state.push_waker.take() {
                waker.wake();
            }

            Ok(PollPull::Computed(computed))
        } else {
            // No batches computed, check if we're done.
            if state.input_finished {
                if state.outer_join_drain_state.is_none() && self.join_requires_drain() {
                    // We don't yet have a drain, check the global state to see
                    // if we can create it.
                    let mut shared = match operator_state {
                        OperatorState::HashJoin(state) => state.inner.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    if shared.probe_inputs_remaining != 0 {
                        // Global state does not yet have all inputs. Need to wait.
                        shared.probe_drain_wakers[state.partition_idx] = Some(cx.waker().clone());
                        return Ok(PollPull::Pending);
                    }

                    let start_idx = state.partition_idx;
                    let skip = shared.probe_partition_count;

                    // Otherwise we can create the drain.
                    let global = match shared.global_outer_join_tracker.as_mut() {
                        Some(global) => global,
                        None => {
                            return Err(RayexecError::new(
                                "Global left outer tracker unexpectedly None",
                            ))
                        }
                    };

                    state.outer_join_drain_state = Some(LeftOuterJoinDrainState::new(
                        start_idx,
                        skip,
                        global.clone(),
                        shared
                            .global_hash_table
                            .as_ref()
                            .unwrap()
                            .collected_batches()
                            .to_vec(),
                        self.right_types.clone(),
                    ));
                }

                // Check if we're still draining unvisited left rows.
                if let Some(drain_state) = state.outer_join_drain_state.as_mut() {
                    if matches!(self.join_type, JoinType::LeftMark { .. }) {
                        // Mark drain
                        match drain_state.drain_mark_next()? {
                            Some(batch) => return Ok(PollPull::Computed(batch.into())),
                            None => return Ok(PollPull::Exhausted),
                        }
                    } else if matches!(self.join_type, JoinType::Semi) {
                        // Semi drain
                        match drain_state.drain_semi_next()? {
                            Some(batch) => return Ok(PollPull::Computed(batch.into())),
                            None => return Ok(PollPull::Exhausted),
                        }
                    } else {
                        // Normal left drain
                        match drain_state.drain_next()? {
                            Some(batch) => return Ok(PollPull::Computed(batch.into())),
                            None => return Ok(PollPull::Exhausted),
                        }
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

impl PhysicalHashJoin {
    /// Inserts a batch into a partition-local hash table.
    fn insert_into_local_table(
        &self,
        state: &mut HashJoinBuildPartitionState,
        batch: BatchOld,
    ) -> Result<()> {
        // Compute left hashes on equality conditions.

        state.hash_buf.clear();
        state.hash_buf.resize(batch.num_rows(), 0);

        for (idx, equality) in self.equalities.iter().enumerate() {
            let result = equality.left.eval(&batch)?;

            if idx == 0 {
                HashExecutor::hash_no_combine(&result, &mut state.hash_buf)?;
            } else {
                HashExecutor::hash_combine(&result, &mut state.hash_buf)?;
            }
        }

        state
            .local_hashtable
            .as_mut()
            .required("partition hash table")?
            .insert_batch(batch, &state.hash_buf)?;

        Ok(())
    }
}

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashJoin")
            .with_values("conditions", &self.conditions)
            .with_values("equalities", &self.equalities)
            .with_value("join_type", self.join_type)
    }
}
