use parking_lot::Mutex;
use rayexec_bullet::array::{Array, UInt64Array};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::compute::take::take;
use rayexec_error::Result;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Waker};

use crate::execution::operators::util::hash::{hash_arrays, partition_for_hash};
use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};

#[derive(Debug)]
pub struct HashRepartitionPartitionState {
    /// Partition index corresponding to this state.
    own_idx: usize,

    /// Buffered output batches for this partition.
    ///
    /// Populated from the the global state to try to minimize locking.
    batches: VecDeque<Batch>,
}

#[derive(Debug)]
pub struct HashRepartitionOperatorState {
    /// Number of input partitions still pushing.
    ///
    /// Once this reaches zero, all output partitions will no longer receive any
    /// batches.
    num_inputs_remaining: AtomicUsize,

    /// Shared states for all partitions.
    shared_states: Vec<Mutex<SharedPartitionState>>,
}

#[derive(Debug)]
struct SharedPartitionState {
    /// Buffered batches.
    batches: VecDeque<Batch>,

    /// Set if batches was empty at time of pull.
    pull_waker: Option<Waker>,

    /// If the input to this partition is finished.
    input_finished: bool,
}

/// Repartition batches based on the hash of some number of columns. The hash
/// values will be appended as an extra column to the batch, allowing for
/// downstream operators to make use of the hashes.
///
/// This repartitioning scheme is very specialized, and exists solely to allow
/// for parallel hash joins and parallel hash aggregates. As such, this
/// repartitioning scheme has the following properties:
///
/// - Input and output numer of partitions are the same.
/// - Unbounded internal buffers for each output partition.
///
/// Unbounded buffers should not be a problem for typical unskewed workloads
/// since the execution model pushes to this operator, then immediately tries to
/// pull from it, and each partition pipeline will be doing an equivalent amount
/// of work. Skewed workloads are something we'll want to look out for.
///
/// If a general purpose 'n' -> 'm' repartitioning scheme is needed, use round
/// robin.
#[derive(Debug)]
pub struct PhysicalHashRepartition {
    /// Columns to hash on the input buffers.
    columns: Vec<usize>,
}

impl PhysicalOperator for PhysicalHashRepartition {
    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        mut batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::HashRepartition(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::HashRepartition(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        let mut hashes = vec![0; batch.num_rows()];

        let arrays: Vec<_> = self
            .columns
            .iter()
            .map(|col_idx| batch.column(*col_idx).expect("column to exist").as_ref())
            .collect();

        hash_arrays(&arrays, &mut hashes)?;

        let num_partitions = operator_state.shared_states.len();
        let partition_indices: Vec<_> = hashes
            .iter()
            .map(|hash| partition_for_hash(*hash, num_partitions))
            .collect();

        // Append hashes colum to batch.
        let hashes = Array::UInt64(UInt64Array::from(hashes));
        batch.try_push_column(hashes)?;

        // Per-partition row indices.
        //
        // We assuming that each batch produces roughly an equal amount of rows
        // per partition.
        let mut row_indices: Vec<Vec<usize>> = (0..num_partitions)
            .map(|_| Vec::with_capacity(batch.num_rows() / num_partitions))
            .collect();

        // Get row indices we'll want to take from the batch for each partition.
        for (row_idx, partition_idx) in partition_indices.into_iter().enumerate() {
            row_indices[partition_idx].push(row_idx);
        }

        for (partition_idx, rows) in row_indices.into_iter().enumerate() {
            // Get the rows we want for this partition.
            let arrays = batch
                .columns()
                .iter()
                .map(|col| take(col, &rows))
                .collect::<Result<Vec<Array>>>()?;
            let partition_batch = Batch::try_new(arrays)?;

            // And send it out.
            let shared = &mut operator_state.shared_states[partition_idx].lock();
            shared.batches.push_back(partition_batch);

            // Wake up if needed.
            if let Some(waker) = shared.pull_waker.take() {
                waker.wake();
            }

            // If this is our partition, move the batches into our local state.
            //
            // This let's us skip locking on a subsequent `poll_pull` since the
            // batch(es) will already be in the partition-local state.
            if partition_idx == state.own_idx {
                state.batches.append(&mut shared.batches);
            }
        }

        Ok(PollPush::Pushed)
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()> {
        let operator_state = match operator_state {
            OperatorState::HashRepartition(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        let prev = operator_state
            .num_inputs_remaining
            .fetch_sub(1, Ordering::SeqCst);

        if prev == 1 {
            for shared in &operator_state.shared_states {
                let shared = &mut shared.lock();
                shared.input_finished = true;
                if let Some(waker) = shared.pull_waker.take() {
                    waker.wake();
                }
            }
        }

        Ok(())
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::HashRepartition(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        // Try getting from our local state first.
        if let Some(batch) = state.batches.pop_front() {
            return Ok(PollPull::Batch(batch));
        }

        let operator_state = match operator_state {
            OperatorState::HashRepartition(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        let shared = &mut operator_state.shared_states[state.own_idx].lock();
        match shared.batches.pop_front() {
            Some(batch) => {
                // Drain from global state into our local state to try to reduce
                // locking on a subsequent pull.
                state.batches.append(&mut shared.batches);

                Ok(PollPull::Batch(batch))
            }
            None => {
                if shared.input_finished {
                    return Ok(PollPull::Exhausted);
                }

                shared.pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
        }
    }
}
