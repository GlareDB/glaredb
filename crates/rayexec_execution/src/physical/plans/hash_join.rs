use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::planner::operator::JoinType;
use crate::types::batch::{DataBatch, DataBatchSchema};

use arrow_array::{BooleanArray, RecordBatch, UInt32Array};
use arrow_schema::Schema;
use hashbrown::raw::RawTable;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use smallvec::{smallvec, SmallVec};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::util::hash::build_hashes;
use super::Sink;

/// Hash join for for joining two tables on column equalities.
///
/// An optional filter provides outputing batches based on an arbitrary
/// expression.
#[derive(Debug)]
pub struct PhysicalHashJoin {
    schema: DataBatchSchema,
    join_type: JoinType,
    // (left, right) indices pairs.
    join_on: Vec<(usize, usize)>,
}

// impl Source2 for PhysicalHashJoin {
//     fn output_partitions(&self) -> usize {
//         self.buffer.output_partitions()
//     }

//     fn poll_partition(
//         &self,
//         cx: &mut Context<'_>,
//         partition: usize,
//     ) -> Poll<Option<Result<DataBatch>>> {
//         self.buffer.poll_partition(cx, partition)
//     }
// }

// impl Sink2 for PhysicalHashJoin {
//     fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
//         unimplemented!()
//     }

//     fn finish(&self, child: usize, partition: usize) -> Result<()> {
//         unimplemented!()
//     }
// }

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalHashJoin")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowKey {
    /// Index of the batch containing this row.
    batch_idx: usize,

    /// Index of the row inside the batch.
    row_idx: usize,
}

#[derive(Default)]
struct LocalHashState {
    /// Partition local table.
    ///
    /// A hash (u64) points to a vector containing the row keys for rows
    /// that match that hash.
    table: RawTable<(u64, SmallVec<[RowKey; 2]>)>,

    /// Collected batches for this partition.
    batches: Vec<DataBatch>,
}

impl fmt::Debug for LocalHashState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalHashState")
            .field("batches", &self.batches)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum HashState {
    /// During the build phase, the partition only has access to its local hash
    /// state.
    Local {
        /// State local to this partition.
        state: LocalHashState,
    },
    /// During probe, the partition should have access to all hash states across
    /// all partitions.
    Global {
        /// All partition states.
        ///
        /// Partition states are indexed by the partition index.
        states: Arc<Vec<LocalHashState>>,
    },
}

impl HashState {
    /// Pushes a batch for the partition local table.
    ///
    /// It's expected that the batches have already been hashed on provided hash
    /// columns.
    fn push_for_build(&mut self, batch: DataBatch, hash_columns: &[usize]) -> Result<()> {
        match self {
            Self::Local { state } => {
                // TODO: These assume that this hash join has a hash repartition
                // operator immediately prior to it.
                let col_hash = batch.get_column_hash().ok_or_else(|| {
                    RayexecError::new("Expected columns to already have been hashed")
                })?;

                if !col_hash.is_for_columns(hash_columns) {
                    return Err(RayexecError::new(
                        "Expected columns to already have been hashed",
                    ));
                }

                let batch_idx = state.batches.len();

                for (row_idx, row_hash) in col_hash.hashes.iter().enumerate() {
                    let row_key = RowKey { batch_idx, row_idx };
                    if let Some((_, indexes)) =
                        state.table.get_mut(*row_hash, |(hash, _)| row_hash == hash)
                    {
                        indexes.push(row_key);
                    } else {
                        state.table.insert(
                            *row_hash,
                            (*row_hash, smallvec![row_key]),
                            |(hash, _)| *hash,
                        );
                    }
                }
                Ok(())
            }
            _ => Err(RayexecError::new("Expected batch state to be local")),
        }
    }

    fn swap_to_global(&mut self, states: Arc<Vec<LocalHashState>>) -> Result<()> {
        match self {
            Self::Local { .. } => {
                *self = HashState::Global { states };
                Ok(())
            }
            _ => Err(RayexecError::new("Expected batch state to be local")),
        }
    }

    fn take_local_state(&mut self) -> Result<LocalHashState> {
        match self {
            Self::Local { state } => Ok(std::mem::take(state)),
            _ => Err(RayexecError::new("Expected batch state to be local")),
        }
    }

    fn get_global_states(&self) -> Result<&Arc<Vec<LocalHashState>>> {
        match self {
            Self::Global { states } => Ok(states),
            _ => Err(RayexecError::new("Expected batch state to be global")),
        }
    }
}

/// State local to a partition.
// TODO: Probably split this up into input/output states.
#[derive(Debug)]
struct LocalState {
    /// If this partition is finished for the build phase.
    build_finished: bool,

    /// If this partition is finished for the probe phase.
    probe_finished: bool,

    /// State of the left side of hash join.
    left_state: HashState,

    /// Waker is for a pending push on the probe side.
    pending_push: Option<Waker>,

    /// Waker is for a pending pull.
    pending_pull: Option<Waker>,

    /// Computed batches.
    computed: VecDeque<DataBatch>,
}

/// Batch sink for the left side of the join.
#[derive(Debug)]
pub struct PhysicalHashJoinBuildSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Number of partitions we're still waiting on to complete.
    remaining: AtomicUsize,

    /// Columns we're hashing on.
    hash_cols: Vec<usize>,
}

impl Sink for PhysicalHashJoinBuildSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_ready(&self, cx: &mut Context, partition: usize) -> Poll<()> {
        // Always need to collect build side.
        Poll::Ready(())
    }

    fn push(&self, input: DataBatch, partition: usize) -> Result<()> {
        // TODO: Probably want to assert that this partition is correct for the
        // input batch.
        let mut state = self.states[partition].lock();
        assert!(!state.build_finished);
        state.left_state.push_for_build(input, &self.hash_cols)?;
        Ok(())
    }

    fn finish(&self, partition: usize) -> Result<()> {
        {
            // Technically just for debugging. We want to make sure we're not
            // accidentally marking the same partition as finished multiple
            // times.
            let state = self.states[partition].lock();
            assert!(!state.build_finished);
        }

        let prev = self.remaining.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            // We're finished, acquire all the locks and build global state.
            let mut states: Vec<_> = self.states.iter().map(|s| s.lock()).collect();

            // Get all the local states.
            let local_states = states
                .iter_mut()
                .map(|s| s.left_state.take_local_state())
                .collect::<Result<Vec<_>>>()?;

            // Update local states to global.
            let local_states = Arc::new(local_states);
            for state in states.iter_mut() {
                state.left_state.swap_to_global(local_states.clone())?;
            }

            // Wake up pending pushes.
            for mut state in states {
                state.build_finished = true;
                if let Some(waker) = state.pending_push.take() {
                    waker.wake();
                }
            }

            // Build is complete.
        }

        Ok(())
    }
}

impl Explainable for PhysicalHashJoinBuildSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhsysicalHashJoinBuildSink")
    }
}

struct ProbeStage {
    batches: Vec<HashedBatch>,
}

impl ProbeStage {
    fn probe(
        &self,
        probe: &RecordBatch,
        key_cols: &[usize],
        join_type: JoinType,
        output_schema: Arc<Schema>,
    ) -> Result<Vec<RecordBatch>> {
        let probe_arrs: Vec<_> = key_cols
            .iter()
            .map(|idx| probe.columns().get(*idx).expect("valid column indexes"))
            .collect();

        let mut hashes = vec![0; probe.num_rows()]; // TODO: Don't allocate every time.
        build_hashes(&probe_arrs, &mut hashes)?;

        let mut output_batches: Vec<RecordBatch> = Vec::with_capacity(self.batches.len());

        for batch in self.batches.iter() {
            // Check equality based on hashes.
            let mut partition_build_indexes: Vec<u32> = Vec::new();
            let mut partition_probe_indexes: Vec<u32> = Vec::new();

            for (probe_row_idx, probe_row_hash) in hashes.iter().enumerate() {
                let entry = batch
                    .table
                    .get(*probe_row_hash, |(hash, _)| probe_row_hash == hash);
                if let Some((_, indexes)) = entry {
                    for build_index in indexes {
                        partition_build_indexes.push(*build_index as u32);
                        partition_probe_indexes.push(probe_row_idx as u32);
                    }
                }
            }

            let mut partition_build_indexes = UInt32Array::from(partition_build_indexes);
            let mut partition_probe_indexes = UInt32Array::from(partition_probe_indexes);

            // Check equality based on key values.
            let build_arrs: Vec<_> = key_cols
                .iter()
                .map(|idx| {
                    batch
                        .batch
                        .columns()
                        .get(*idx)
                        .expect("valid column indexes")
                })
                .collect();

            let mut equal = BooleanArray::from(vec![true; partition_probe_indexes.len()]);
            let iter = build_arrs.iter().zip(probe_arrs.iter());
            equal = iter
                .map(|(build, probe)| {
                    let build_take = arrow::compute::take(&build, &partition_build_indexes, None)?;
                    let probe_take = arrow::compute::take(&probe, &partition_probe_indexes, None)?;
                    // TODO: null == null
                    arrow_ord::cmp::eq(&build_take, &probe_take)
                })
                .try_fold(equal, |acc, result| arrow::compute::and(&acc, &result?))?;

            partition_build_indexes = arrow_array::cast::downcast_array(&arrow::compute::filter(
                &partition_build_indexes,
                &equal,
            )?);
            partition_probe_indexes = arrow_array::cast::downcast_array(&arrow::compute::filter(
                &partition_probe_indexes,
                &equal,
            )?);

            // Adjust indexes as needed depending on the join type.
            // TODO: Handle everything else.
            match join_type {
                JoinType::Inner => {
                    // No adjustments needed.
                }
                _ => unimplemented!(),
            }

            // Build final record batch.
            let mut cols = Vec::with_capacity(output_schema.fields.len());
            for build_col in batch.batch.columns() {
                cols.push(arrow::compute::take(
                    build_col,
                    &partition_build_indexes,
                    None,
                )?);
            }
            for probe_col in probe.columns() {
                cols.push(arrow::compute::take(
                    probe_col,
                    &partition_probe_indexes,
                    None,
                )?)
            }

            let output = RecordBatch::try_new(output_schema.clone(), cols)?;
            output_batches.push(output);
        }

        Ok(output_batches)
    }
}

/// A record batch along with key hashes.
struct HashedBatch {
    table: RawTable<(u64, SmallVec<[usize; 2]>)>,
    batch: RecordBatch,
}
