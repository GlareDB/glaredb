mod hashtable;

use crate::physical::plans::hash_aggregate::hashtable::GroupingSetColumns;
use crate::physical::plans::util::hash::{build_hashes, hash_partition_batch};
use crate::physical::TaskContext;
use crate::types::batch::DataBatch;
use crate::{
    functions::aggregate::Accumulator,
    planner::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use hashbrown::raw::RawTable;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use smallvec::{smallvec, SmallVec};
use std::fmt;
use std::task::{Context, Waker};

use self::hashtable::{GroupingSetAccumulators, GroupingSetHashTable};

use super::{PollPush, Sink};

#[derive(Debug)]
pub struct GroupingSets {
    /// All distinct columns used in all of the grouping sets.
    columns: Vec<usize>,

    /// Masks for indicating groups.
    ///
    /// Each vector should be the same size as the above columns, and each value
    /// indicates if that column is part of the group or not.
    groups: Vec<Vec<bool>>,
}

impl GroupingSets {
    pub fn try_new(columns: Vec<usize>, groups: Vec<Vec<bool>>) -> Result<Self> {
        for group in &groups {
            if group.len() != columns.len() {
                return Err(RayexecError::new(format!(
                    "Unexpected group size of {}, expected {}",
                    group.len(),
                    columns.len()
                )));
            }
        }

        Ok(GroupingSets { columns, groups })
    }
}

#[derive(Debug)]
pub struct PhysicalHashAggregate {}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalHashAggregate")
    }
}

/// Input state for a single grouping set.
///
/// This internally will build multiple hash tables, one for each output
/// partition, and write the inputs based on the hash value for the group key.
///
/// When the input partition is finished, each table in this state will be
/// merged with the correct output partition table.
#[derive(Debug)]
struct InputGroupingSetState {
    /// Columns that make up this grouping set.
    columns: Vec<usize>,

    /// Hash tables holding the grouping set keys.
    ///
    /// Each hash table corresponds to the _output_ partition based on some
    /// hashing scheme.
    hash_tables: Vec<GroupingSetHashTable>,

    /// Accumulator states.
    ///
    /// Each set of accumulators corresponds to the _output_ partition, just
    /// like the hash table.
    accumulators: Vec<GroupingSetAccumulators>,
}

/// Output state for a single grouping set.
///
/// This contains only the hash table and group values for this partition.
#[derive(Debug)]
struct OutputGroupingSetState {
    /// Columns that make up this grouping set.
    columns: Vec<usize>,

    /// The hash table for this grouping set.
    hash_table: GroupingSetHashTable,

    /// The accumulators for this grouping set.
    accumulators: GroupingSetAccumulators,
}

/// State for input partitions.
#[derive(Debug)]
struct InputPartitionState {
    grouping_sets: Vec<InputGroupingSetState>,
}

/// State for the output partitions.
#[derive(Debug)]
struct OutputPartitionState {
    /// Waker from the pull side.
    pull_waker: Option<Waker>,

    /// Output states for all the grouping sets.
    grouping_sets: Vec<OutputGroupingSetState>,

    /// Number of inputs still remaining before output can be generated.
    inputs_remaining: usize,
}

#[derive(Debug)]
pub struct PhysicalHashAggregateSink {
    /// Partition states while aggregating inputs.
    input_states: Vec<Mutex<InputPartitionState>>,

    output_states: Vec<Mutex<OutputPartitionState>>,

    /// Number of partitions we're working with.
    partitions: usize,
}

impl Sink for PhysicalHashAggregateSink {
    fn input_partitions(&self) -> usize {
        self.partitions
    }

    fn poll_push(
        &self,
        _task_cx: &TaskContext,
        cx: &mut Context,
        input: DataBatch,
        partition: usize,
    ) -> Result<PollPush> {
        let mut state = self.input_states[partition].lock();

        let mut hashes = vec![0; input.num_rows()];
        let mut groups = Vec::new();

        // Update all grouping set states from the input. Each input batch will
        // be hash partitioned such that they can be placed in their expected
        // "output" tables.
        for grouping_set in state.grouping_sets.iter_mut() {
            let arrs: Vec<_> = grouping_set
                .columns
                .iter()
                .map(|idx| input.column(*idx).expect("column to exist on input"))
                .collect();
            let hashes = build_hashes(&arrs, &mut hashes)?;

            let partition_batches = hash_partition_batch(&input, hashes, self.partitions)?;
            for (partition_idx, batch) in partition_batches.into_iter().enumerate() {
                // Update hash table for this grouping set. This will write the
                // computed groups based on the hash and actual row equality
                // into `groups.
                let group_by_cols: Vec<_> = grouping_set
                    .columns
                    .iter()
                    .map(|idx| {
                        batch
                            .batch
                            .column(*idx)
                            .expect("column to exist on hash partitioned batch")
                            .clone()
                    })
                    .collect();
                let cols = GroupingSetColumns {
                    columns: &group_by_cols,
                    hashes: &batch.hashes,
                };
                grouping_set.hash_tables[partition_idx].insert_groups(cols, &mut groups)?;

                // Now update into the accumulators.
                grouping_set.accumulators[partition_idx].update_groups(&batch.batch, &groups)?;
            }
        }

        Ok(PollPush::Pushed)
    }

    fn finish(&self, task_cx: &TaskContext, partition: usize) -> Result<()> {
        // TODO: Think, just an outline right now.

        let mut state = self.input_states[partition].lock();

        // For each table we created locally, merge it with the output table.
        for output_idx in 0..self.partitions {
            let mut output_state = self.output_states[output_idx].lock();

            for (input, output) in state
                .grouping_sets
                .iter_mut()
                .zip(output_state.grouping_sets.iter_mut())
            {
                output
                    .hash_table
                    .merge_from(&mut input.hash_tables[output_idx])?;

                output
                    .accumulators
                    .merge_from(&mut input.accumulators[output_idx])?;
            }

            output_state.inputs_remaining -= 1;

            // If we were the last input partition for this output partition, go
            // ahead and wake up pending tasks.
            if output_state.inputs_remaining == 0 {
                if let Some(waker) = output_state.pull_waker.take() {
                    waker.wake();
                }
            }
        }

        Ok(())
    }
}

impl Explainable for PhysicalHashAggregateSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalHashAggregateSink")
    }
}
