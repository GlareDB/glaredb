use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::planner::operator::JoinType;
use crate::types::batch::DataBatch;
use arrow_array::{BooleanArray, UInt32Array};
use hashbrown::raw::RawTable;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use smallvec::{smallvec, SmallVec};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::{Sink, Source};

/// Hash join for for joining two tables on column equalities with pre-hashed
/// batches partitioned on the hash.
///
/// This requires that the incomding batches for both the build and probe sinks
/// are partitioned by `partition_for_hash`. This allows partitions-local hash
/// tables to be built without needing to consult the hash table for other
/// partitions for a batch.
///
/// When the left side hash table has been built for each partition, each
/// partition-local state transitions into a "global" state containing a
/// reference to _all_ hash tables across all batches. This simply takes the
/// hash table from each partition and appends it to a vector. We do not have to
/// rebuild the hash or make alterations to any of the left batches when this is
/// done, making the operation (hypothetically) very fast.
///
/// During the probe phase, we take advantage of the hash-based partitioning to
/// quickly index to the correct hash table for that partition, and build the
/// joined batches by just referencing a single hash table during the probe.
//
// TODO: Probably need to add a filter since we'll need to be able to handle
// arbitrary join conditions and left/right/anti joins will need to know which
// rows have been visited based on the join condition.
//
// TODO: Check assumptions around performance of hash-repartitioning on the
// probe side. It may make sense to just doe round robin, and handle the batch
// containing matches from multiple partitions in this operator. Each partition
// already has a reference to all hash tables, so the change would be minimally
// invasive (but non-trivial since the logic in probe will get more complex).
//
// TODO: Understand potential benefits of guaranteeing output partitioning. For
// example if we're joining on a column, then follow up with an aggregate
// grouped by that column, we could just reuse the hashes and existing knowledge
// of what batch is in a partition.
//
#[derive(Debug)]
pub struct PhysicalPartitionedHashJoin {
    /// Columns on the left side to join on.
    left_on: Vec<usize>,

    /// Columns on the right side to join on.
    right_on: Vec<usize>,

    join_type: JoinType,

    /// Shared states across the sinks and source for all partitions.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// The configured build sink. Expected to be taken during pipeline
    /// building.
    build_sink: Option<PhysicalPartitionedHashJoinBuildSink>,

    /// The configured probe sink. Expected to be taken during pipeline
    /// building.
    probe_sink: Option<PhysicalPartitionedHashJoinProbeSink>,
}

impl PhysicalPartitionedHashJoin {
    pub fn try_new(
        left_on: Vec<usize>,
        right_on: Vec<usize>,
        partitions: usize,
        join_type: JoinType,
    ) -> Result<Self> {
        if left_on.len() != right_on.len() {
            return Err(RayexecError::new(
                "Left and right column indexes must be the same length",
            ));
        }

        let states: Vec<_> = (0..partitions)
            .map(|_| {
                Mutex::new(LocalState {
                    build_finished: false,
                    probe_finished: false,
                    left_state: HashState::Local {
                        state: LocalHashState {
                            table: RawTable::new(),
                            batches: Vec::new(),
                        },
                    },
                    pending_push: None,
                    pending_pull: None,
                    computed: VecDeque::new(),
                })
            })
            .collect();

        let states = Arc::new(states);
        let build_sink = PhysicalPartitionedHashJoinBuildSink {
            states: states.clone(),
            remaining: partitions.into(),
            hash_cols: left_on.clone(),
        };
        let probe_sink = PhysicalPartitionedHashJoinProbeSink {
            states: states.clone(),
            hash_cols: right_on.clone(),
            join_type,
        };

        Ok(PhysicalPartitionedHashJoin {
            left_on,
            right_on,
            join_type,
            states,
            build_sink: Some(build_sink),
            probe_sink: Some(probe_sink),
        })
    }

    pub fn take_build_sink(&mut self) -> Option<PhysicalPartitionedHashJoinBuildSink> {
        self.build_sink.take()
    }

    pub fn take_probe_sink(&mut self) -> Option<PhysicalPartitionedHashJoinProbeSink> {
        self.probe_sink.take()
    }
}

impl Source for PhysicalPartitionedHashJoin {
    fn output_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_next(&self, cx: &mut Context, partition: usize) -> Poll<Option<Result<DataBatch>>> {
        let mut state = self.states[partition].lock();
        if !state.build_finished {
            state.pending_pull = Some(cx.waker().clone());
            return Poll::Pending;
        }

        if let Some(batch) = state.computed.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }

        if state.probe_finished {
            return Poll::Ready(None);
        }

        state.pending_pull = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Explainable for PhysicalPartitionedHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalPartitionedHashJoin")
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
pub struct PhysicalPartitionedHashJoinBuildSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Number of partitions we're still waiting on to complete.
    remaining: AtomicUsize,

    /// Columns we're hashing on.
    hash_cols: Vec<usize>,
}

impl Sink for PhysicalPartitionedHashJoinBuildSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_ready(&self, _cx: &mut Context, _partition: usize) -> Poll<()> {
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

impl Explainable for PhysicalPartitionedHashJoinBuildSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalPartitionedHashJoinBuildSink")
    }
}

#[derive(Debug)]
pub struct PhysicalPartitionedHashJoinProbeSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Column indexes that have been hashed.
    hash_cols: Vec<usize>,

    join_type: JoinType,
}

impl Sink for PhysicalPartitionedHashJoinProbeSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_ready(&self, cx: &mut Context, partition: usize) -> Poll<()> {
        let mut state = self.states[partition].lock();
        if !state.build_finished {
            // We're still building, register for a wakeup.
            state.pending_push = Some(cx.waker().clone());
            return Poll::Pending;
        }
        assert!(!state.probe_finished);
        Poll::Ready(())
    }

    fn push(&self, input: DataBatch, partition: usize) -> Result<()> {
        let local_states = {
            let state = self.states[partition].lock();
            assert!(state.build_finished);
            state.left_state.get_global_states()?.clone()
        };

        let outputs = probe(
            &local_states,
            partition,
            &input,
            &self.hash_cols,
            self.join_type,
        )?;

        let mut state = self.states[partition].lock();
        state.computed.extend(outputs);

        if let Some(waker) = state.pending_pull.take() {
            waker.wake();
        }

        Ok(())
    }

    fn finish(&self, partition: usize) -> Result<()> {
        let mut state = self.states[partition].lock();
        assert!(state.build_finished);
        assert!(!state.probe_finished);
        state.probe_finished = true;

        if let Some(waker) = state.pending_pull.take() {
            waker.wake();
        }

        Ok(())
    }
}

impl Explainable for PhysicalPartitionedHashJoinProbeSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalPartitionedHashJoinProbeSink")
    }
}

/// Probe the left side of the join with the "right" input batch.
///
/// This assumes that the "right" batch was created for the given partition
/// using the same hash repartitioning schema as the build side.
fn probe(
    left: &[LocalHashState],
    partition: usize,
    right: &DataBatch,
    join_cols: &[usize],
    join_type: JoinType,
) -> Result<Vec<DataBatch>> {
    // Assumes the right side has already been hashed.
    let right_hash = right.get_column_hash().ok_or_else(|| {
        RayexecError::new("Expected columns in right side of join to already have been hashed")
    })?;
    if !right_hash.is_for_columns(join_cols) {
        return Err(RayexecError::new(
            "Expected columns in right side of join to already have been hashed",
        ));
    }

    let mut outputs = Vec::new();

    // Get the partition state that corresponds to these hashes.
    let state = left.get(partition).expect("left partition to exist");

    // Indexes into the left and right batches. Note that this stored in a map
    // since one hash value may correspond to rows in different batches on the
    // left side. The key just indicates which of those batches contains the
    // match, and the value is the indexes of rows in that batch.
    //
    // TODO: It might make sense to just concat the batches on the left side
    // into a single large batch. _Or_ adjust row key to be a single usize and
    // account for preceding batch lengths.
    let mut partition_build_indexes: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut partition_probe_indexes: BTreeMap<usize, Vec<usize>> = BTreeMap::new();

    for (probe_row_idx, probe_row_hash) in right_hash.hashes.iter().enumerate() {
        let entry = state
            .table
            .get(*probe_row_hash, |(hash, _)| probe_row_hash == hash);
        if let Some((_, indexes)) = entry {
            for row_key in indexes {
                use std::collections::btree_map::Entry;

                match partition_build_indexes.entry(row_key.batch_idx) {
                    Entry::Vacant(ent) => {
                        ent.insert(vec![row_key.row_idx]);

                        let existing =
                            partition_probe_indexes.insert(row_key.batch_idx, vec![probe_row_idx]);
                        assert!(
                            existing.is_none(),
                            "build indexes out of sync, previous value exists"
                        );
                    }
                    Entry::Occupied(mut ent) => {
                        let build_rows = ent.get_mut();
                        build_rows.push(row_key.row_idx);

                        let probe_rows = partition_probe_indexes
                            .get_mut(&row_key.batch_idx)
                            .expect("probe row indexes should already exist for this batch");
                        probe_rows.push(probe_row_idx);
                    }
                }
            }
        }
    }

    for ((build_idx1, build), (build_idx2, probe)) in partition_build_indexes
        .into_iter()
        .zip(partition_probe_indexes.into_iter())
    {
        assert_eq!(build_idx1, build_idx2);
        let build_batch = state.batches.get(build_idx1).expect("build batch to exist");

        let mut partition_build_indexes =
            UInt32Array::from_iter(build.into_iter().map(|idx| idx as u32));
        let mut partition_probe_indexes =
            UInt32Array::from_iter(probe.into_iter().map(|idx| idx as u32));

        // Check equality based on key values.
        let build_arrs: Vec<_> = join_cols
            .iter()
            .map(|idx| {
                build_batch
                    .column(*idx)
                    .expect("column in build batch to exist")
            })
            .collect();
        let probe_arrs: Vec<_> = join_cols
            .iter()
            .map(|idx| right.column(*idx).expect("column in probe batch to exist"))
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
        match join_type {
            JoinType::Inner => {
                // No adjustments needed.
            }
            _ => unimplemented!(),
        }

        let mut cols = Vec::new();
        for build_col in build_batch.columns() {
            cols.push(arrow::compute::take(
                build_col,
                &partition_build_indexes,
                None,
            )?);
        }
        for probe_col in right.columns() {
            cols.push(arrow::compute::take(
                probe_col,
                &partition_probe_indexes,
                None,
            )?)
        }

        let batch = DataBatch::try_new(cols)?;
        outputs.push(batch);
    }

    Ok(outputs)
}
