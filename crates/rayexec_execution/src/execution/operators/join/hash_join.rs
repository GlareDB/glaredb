use parking_lot::Mutex;
use rayexec_bullet::array::{Array, BooleanArray};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::compute::filter::filter;
use rayexec_error::{RayexecError, Result};
use std::task::Context;
use std::{sync::Arc, task::Waker};

use crate::execution::operators::util::hash::{hash_arrays, partition_for_hash};
use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};
use crate::planner::operator::JoinType;

use super::join_hash_table::PartitionJoinHashTable;

#[derive(Debug)]
pub struct HashJoinBuildPartitionState {
    /// Hash table this partition will be writing to.
    local_hashtable: PartitionJoinHashTable,

    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
}

impl HashJoinBuildPartitionState {
    fn new() -> Self {
        HashJoinBuildPartitionState {
            local_hashtable: PartitionJoinHashTable::new(),
            hash_buf: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct HashJoinProbePartitionState {
    /// Index of this partition.
    partition_idx: usize,

    /// The final output table. If None, the global state should be checked to
    /// see if it's ready to copy into the partition local state.
    global: Option<Arc<PartitionJoinHashTable>>,

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
}

impl HashJoinProbePartitionState {
    fn new(partition_idx: usize) -> Self {
        HashJoinProbePartitionState {
            partition_idx,
            global: None,
            hash_buf: Vec::new(),
            buffered_output: None,
            push_waker: None,
            pull_waker: None,
            input_finished: false,
        }
    }
}

#[derive(Debug)]
pub struct HashJoinOperatorState {
    /// Shared state between all partitions.
    inner: Mutex<SharedOutputState>,
}

#[derive(Debug)]
struct SharedOutputState {
    /// The partially built global hash table.
    ///
    /// Input partitions merge their partition-local hash table into this global
    /// table once they complete.
    partial: PartitionJoinHashTable,

    /// Number of build inputs remaining.
    ///
    /// Initially set to number of build partitions.
    remaining: usize,

    /// The shared global hash table once it's been fully built.
    ///
    /// This is None if there's still inputs still building.
    shared_global: Option<Arc<PartitionJoinHashTable>>,

    /// Pending wakers for thread that attempted to probe the table prior to it
    /// being built.
    ///
    /// Indexed by probe partition index.
    ///
    /// Woken once the global hash table has been completed (moved into
    /// `shared_global`).
    probe_push_wakers: Vec<Option<Waker>>,
}

impl SharedOutputState {
    fn new(build_partitions: usize, probe_partitions: usize) -> Self {
        SharedOutputState {
            partial: PartitionJoinHashTable::new(),
            remaining: build_partitions,
            shared_global: None,
            probe_push_wakers: vec![None; probe_partitions],
        }
    }
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    /// The type of join we're performing (inner, left, right, semi, etc).
    join_type: JoinType,

    /// Column indices on the left (build) side we're joining on.
    left_on: Vec<usize>,

    /// Column indices on the right (probe) side we're joining on.
    right_on: Vec<usize>,
}

impl PhysicalHashJoin {
    pub fn new(join_type: JoinType, left_on: Vec<usize>, right_on: Vec<usize>) -> Self {
        PhysicalHashJoin {
            join_type,
            left_on,
            right_on,
        }
    }

    /// Create states for this operator.
    ///
    /// The number of partition inputs on the build side may be different than
    /// the number of partitions on the probe side.
    ///
    /// Output partitions equals the number of probe side input partitions.
    pub fn create_states(
        &self,
        build_partitions: usize,
        probe_partitions: usize,
    ) -> (
        HashJoinOperatorState,
        Vec<HashJoinBuildPartitionState>,
        Vec<HashJoinProbePartitionState>,
    ) {
        let operator_state = HashJoinOperatorState {
            inner: Mutex::new(SharedOutputState::new(build_partitions, probe_partitions)),
        };

        let build_states: Vec<_> = (0..build_partitions)
            .map(|_| HashJoinBuildPartitionState::new())
            .collect();

        let probe_states: Vec<_> = (0..probe_partitions)
            .map(HashJoinProbePartitionState::new)
            .collect();

        (operator_state, build_states, probe_states)
    }
}

impl PhysicalOperator for PhysicalHashJoin {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::HashJoinBuild(state) => {
                let left_columns = self
                    .left_on
                    .iter()
                    .map(|idx| {
                        batch.column(*idx).map(|arr| arr.as_ref()).ok_or_else(|| {
                            RayexecError::new(format!("Missing column at index {idx}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Compute hashes on input batch
                state.hash_buf.clear();
                state.hash_buf.resize(batch.num_rows(), 0);
                let hashes = hash_arrays(&left_columns, &mut state.hash_buf)?;

                state.local_hashtable.insert_batch(
                    &batch,
                    hashes,
                    Bitmap::all_true(hashes.len()),
                )?;

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
                    if shared.remaining != 0 {
                        shared.probe_push_wakers[state.partition_idx] = Some(cx.waker().clone());
                        return Ok(PollPush::Pending(batch));
                    }

                    // Final partition on the build side should be what sets
                    // this. So if remaining == 0, then it should exist.
                    let shared_global = shared
                        .shared_global
                        .clone()
                        .expect("shared global table should exist, no inputs remaining");

                    // Final hash table built, store in our partition local
                    // state.
                    state.global = Some(shared_global);
                }

                let hashtable = state.global.as_ref().expect("hash table to exist");

                let right_input_cols = self
                    .right_on
                    .iter()
                    .map(|idx| {
                        batch.column(*idx).map(|arr| arr.as_ref()).ok_or_else(|| {
                            RayexecError::new(format!(
                                "Missing column in probe batch at index {idx}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                state.hash_buf.clear();
                state.hash_buf.resize(batch.num_rows(), 0);
                let hashes = hash_arrays(&right_input_cols, &mut state.hash_buf)?;

                // TODO: Handle everything else.
                //
                // Left:
                // - Include every unvisited row in left batch, join with right nulls.
                // - Partition local bitmap to track unvisited left batchs.
                // - Flush out unvisited batches on finish.
                //
                // Right:
                // - Include every unvisited row in right batch, join with left nulls.
                // - Nothing else.
                //
                // Outer:
                // - Include every unvisited row in right batch, join with left nulls.
                // - Include every unvisited row in left batch, join with right nulls,
                // - Partition local bitmap to track unvisited left batchs.
                // - Flush out unvisited batches on finish.
                //
                // Left/right semi:
                // - Just include left/right columns.
                //
                // Left/right anti:
                // - Inverse of left/right
                match self.join_type {
                    JoinType::Inner => {
                        let joined = hashtable.probe(&batch, hashes, &self.right_on)?;
                        state.buffered_output = Some(joined);
                        Ok(PollPush::Pushed)
                    }
                    JoinType::Left => {
                        unimplemented!()
                    }
                    JoinType::Right => {
                        unimplemented!()
                    }
                    JoinType::Full => {
                        unimplemented!()
                    }
                }
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
            PartitionState::HashJoinBuild(state) => {
                let operator_state = match operator_state {
                    OperatorState::HashJoin(state) => state,
                    other => panic!("invalid operator state: {other:?}"),
                };

                // Merge local table into the global table.
                let local_table =
                    std::mem::replace(&mut state.local_hashtable, PartitionJoinHashTable::new());

                let mut shared = operator_state.inner.lock();
                shared.partial.merge(local_table)?;

                shared.remaining -= 1;

                // If we're the last remaining, go ahead and move the 'partial'
                // table to 'global', and wake up any pending probers.
                //
                // Probers will then clone the global hash table (behind an Arc)
                // into their local states to avoid needing to synchronize.
                if shared.remaining == 0 {
                    let global_table =
                        std::mem::replace(&mut shared.partial, PartitionJoinHashTable::new());
                    shared.shared_global = Some(Arc::new(global_table));

                    for waker in shared.probe_push_wakers.iter_mut() {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }
                }

                Ok(())
            }
            PartitionState::HashJoinProbe(state) => {
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
