pub mod aggregate_hash_table;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::task::{Context, Waker};

use aggregate_hash_table::{AggregateHashTableDrain, AggregateStates, PartitionAggregateHashTable};
use parking_lot::Mutex;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::scalar::HashExecutor;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::{RayexecError, Result};

use super::util::resizer::{BatchResizer, DEFAULT_TARGET_BATCH_SIZE};
use super::{ExecutionStates, InputOutputStates, PollFinalize};
use crate::database::DatabaseContext;
use crate::execution::computed_batch::ComputedBatches;
use crate::execution::operators::util::hash::partition_for_hash;
use crate::execution::operators::{
    ExecutableOperator,
    OperatorState,
    PartitionState,
    PollPull,
    PollPush,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalAggregateExpression;

#[derive(Debug)]
pub enum HashAggregatePartitionState {
    /// Partition is currently aggregating inputs.
    Aggregating(AggregatingPartitionState),
    /// Partition is currently producing final aggregate results.
    Producing(ProducingPartitionState),
}

#[derive(Debug)]
pub struct AggregatingPartitionState {
    /// Resizer for buffering input batches during aggregation.
    resizer: BatchResizer,
    /// Index of this partition.
    partition_idx: usize,
    /// Output hash tables for storing aggregate states.
    ///
    /// There exists one hash table per output partition.
    output_hashtables: Vec<PartitionAggregateHashTable>,
    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
    /// Resusable partitions buffer.
    partitions_idx_buf: Vec<usize>,
}

#[derive(Debug)]
pub struct ProducingPartitionState {
    /// Index of this partition.
    partition_idx: usize,
    /// The aggregate hash table that we're pulling results from.
    ///
    /// May be None if the final hash table hasn't been built yet. If it
    /// hasn't been built, then the shared state will be need to be checked.
    hashtable_drain: Option<AggregateHashTableDrain>,
}

impl HashAggregatePartitionState {
    fn partition_idx(&self) -> usize {
        match self {
            HashAggregatePartitionState::Aggregating(s) => s.partition_idx,
            HashAggregatePartitionState::Producing(s) => s.partition_idx,
        }
    }
}

#[derive(Debug)]
pub struct HashAggregateOperatorState {
    /// States containing pending hash tables from input partitions.
    output_states: Vec<Mutex<SharedOutputPartitionState>>,
}

#[derive(Debug)]
struct SharedOutputPartitionState {
    /// Completed hash tables from input partitions that should be combined into
    /// one final output table.
    completed: Vec<PartitionAggregateHashTable>,

    /// Number of remaining inputs. Initially set to number of input partitions.
    ///
    /// Once zero, the final hash table can be created.
    remaining: usize,

    /// Waker for thread that attempted to pull from this operator before we've
    /// completed the aggregation.
    pull_waker: Option<Waker>,
}

/// Compute aggregates over input batches.
///
/// Output batch columns will include the computed aggregate, followed by the
/// group by columns.
#[derive(Debug)]
pub struct PhysicalHashAggregate {
    /// Null masks for determining which column values should be part of a
    /// group.
    null_masks: Vec<Bitmap>,
    /// Distinct columns that are used in the grouping sets.
    group_columns: Vec<usize>,
    /// Datatypes of the columns in the grouping sets.
    group_types: Vec<DataType>,
    /// Union of all column indices that are inputs to the aggregate functions.
    aggregate_columns: Vec<usize>,
    exprs: Vec<PhysicalAggregateExpression>,
}

impl PhysicalHashAggregate {
    pub fn new(
        group_types: Vec<DataType>,
        exprs: Vec<PhysicalAggregateExpression>,
        grouping_sets: Vec<BTreeSet<usize>>,
    ) -> Self {
        // Collect all unique column indices that are part of computing the
        // aggregate.
        let mut agg_input_cols = BTreeSet::new();
        for expr in &exprs {
            agg_input_cols.extend(expr.columns.iter().map(|expr| expr.idx));
        }

        // Used to generate intial null masks. This doesn't take into account
        // the physical column offset.
        let mut distinct_group_cols = BTreeSet::new();
        for set in &grouping_sets {
            distinct_group_cols.extend(set.iter());
        }

        let null_masks = grouping_sets
            .iter()
            .map(|set| {
                let mut mask = Bitmap::new_with_all_false(distinct_group_cols.len());
                for (idx, col_idx) in distinct_group_cols.iter().enumerate() {
                    mask.set_unchecked(idx, !set.contains(col_idx))
                }
                mask
            })
            .collect();

        // Adjust group cols to take into account physical column index.
        let group_columns = distinct_group_cols
            .into_iter()
            .map(|col| col + agg_input_cols.len())
            .collect();

        PhysicalHashAggregate {
            null_masks,
            group_columns,
            group_types,
            aggregate_columns: agg_input_cols.into_iter().collect(),
            exprs,
        }
    }
}

impl ExecutableOperator for PhysicalHashAggregate {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let num_partitions = partitions[0];

        // Create column selection bitmaps for each aggregate expression. These
        // bitmaps are used to mask input columns into the operator.
        let mut col_selections = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            let col_selection = Bitmap::from_iter(
                self.aggregate_columns
                    .iter()
                    .map(|idx| expr.contains_column_idx(*idx)),
            );
            col_selections.push(col_selection);
        }

        let operator_state = OperatorState::HashAggregate(HashAggregateOperatorState {
            output_states: (0..num_partitions)
                .map(|_| {
                    Mutex::new(SharedOutputPartitionState {
                        completed: Vec::new(),
                        remaining: num_partitions,
                        pull_waker: None,
                    })
                })
                .collect(),
        });

        let mut partition_states = Vec::with_capacity(num_partitions);
        for idx in 0..num_partitions {
            let partition_local_tables = (0..num_partitions)
                .map(|_| {
                    let agg_states: Vec<_> = self
                        .exprs
                        .iter()
                        .zip(col_selections.iter())
                        .map(|(expr, col_selection)| AggregateStates {
                            states: expr.function.new_grouped_state(),
                            col_selection: col_selection.clone(),
                        })
                        .collect();
                    PartitionAggregateHashTable::try_new(agg_states)
                })
                .collect::<Result<Vec<_>>>()?;

            let partition_state = PartitionState::HashAggregate(
                HashAggregatePartitionState::Aggregating(AggregatingPartitionState {
                    resizer: BatchResizer::new(DEFAULT_TARGET_BATCH_SIZE),
                    partition_idx: idx,
                    output_hashtables: partition_local_tables,
                    hash_buf: Vec::new(),
                    partitions_idx_buf: Vec::new(),
                }),
            );

            partition_states.push(partition_state);
        }

        Ok(ExecutionStates {
            operator_state: Arc::new(operator_state),
            partition_states: InputOutputStates::OneToOne { partition_states },
        })
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Aggregating(state) => {
                self.push_batch_for_aggregating(state, batch)?;

                // Aggregates don't produce anything until it's been finalized.
                Ok(PollPush::NeedsMore)
            }
            HashAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to push to partition that should be producing batches",
            )),
        }
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::HashAggregate(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Aggregating(agg_state) => {
                // Flush any pending batches.
                self.flush_pending_aggregate_batches(agg_state)?;

                // Set this partition's state to producing with an empty hash
                // table.
                //
                // On pull, this partition will build the final hash table from
                // the global state if all inputs are finished, or store a waker
                // if not.
                let producing_state =
                    HashAggregatePartitionState::Producing(ProducingPartitionState {
                        partition_idx: state.partition_idx(),
                        hashtable_drain: None,
                    });
                let aggregating_state = std::mem::replace(state, producing_state);
                let partition_hashtables = match aggregating_state {
                    HashAggregatePartitionState::Aggregating(state) => state.output_hashtables,
                    _ => unreachable!("state variant already checked in outer match"),
                };

                for (partition_idx, partition_hashtable) in
                    partition_hashtables.into_iter().enumerate()
                {
                    let mut output_state = operator_state.output_states[partition_idx].lock();
                    output_state.completed.push(partition_hashtable);

                    output_state.remaining -= 1;

                    // If we're the last input partition for an output
                    // partition, go ahead a wake up whoever is waiting.
                    if let Some(waker) = output_state.pull_waker.take() {
                        waker.wake();
                    }
                }

                Ok(PollFinalize::Finalized)
            }
            HashAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to finalize a partition that's producing output",
            )),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::HashAggregate(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Producing(state) => {
                // Check if we have the finaly hash table. Try to build it if we
                // don't.
                if state.hashtable_drain.is_none() {
                    let mut shared_state = operator_state.output_states[state.partition_idx].lock();
                    if shared_state.remaining != 0 {
                        // Still need to wait for some input partitions to complete. Store our
                        // waker and come back later.
                        shared_state.pull_waker = Some(cx.waker().clone());
                        return Ok(PollPull::Pending);
                    }

                    // Othewise let's build the final table. Note that
                    // continuing to hold the lock here is fine since all inputs
                    // have completed and so won't try to acquire it.
                    let completed = std::mem::take(&mut shared_state.completed);
                    let mut completed_iter = completed.into_iter();
                    let mut first = completed_iter
                        .next()
                        .expect("there to be at least one partition");

                    for consume in completed_iter {
                        first.merge(consume)?;
                    }

                    let drain = first.into_drain(1024, self.group_types.clone()); // TODO: Make batch size configurable.
                    state.hashtable_drain = Some(drain);
                }

                // Drain should be Some by here.
                match state.hashtable_drain.as_mut().unwrap().next() {
                    Some(Ok(batch)) => Ok(PollPull::Computed(batch.into())),
                    Some(Err(e)) => Err(e),
                    None => Ok(PollPull::Exhausted),
                }
            }
            HashAggregatePartitionState::Aggregating(state) => {
                let mut shared = operator_state.output_states[state.partition_idx].lock();
                shared.pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
        }
    }
}

impl PhysicalHashAggregate {
    /// Pushes a batch for aggregating.
    ///
    /// This may buffer the batch until additional batches come to produce a
    /// batch of the desired target size.
    fn push_batch_for_aggregating(
        &self,
        state: &mut AggregatingPartitionState,
        batch: Batch,
    ) -> Result<()> {
        // TODO: Resizer has a negative impact on perf here. This makes sense,
        // but could be something we look at closer. Currently we just insert
        // directly into the hash table.
        self.insert_batch_agg_hash_table(state, batch)?;

        // match state.resizer.try_push(batch)? {
        //     ComputedBatches::Single(batch) => self.insert_batch_agg_hash_table(state, batch)?,
        //     ComputedBatches::Multi(batches) => {
        //         for batch in batches {
        //             self.insert_batch_agg_hash_table(state, batch)?;
        //         }
        //     }
        //     ComputedBatches::None => (), // Not enough rows buffered yet.
        // }

        Ok(())
    }

    /// Flush any pending batches from the resizer into the partition-local
    /// aggregate hash table.
    fn flush_pending_aggregate_batches(&self, state: &mut AggregatingPartitionState) -> Result<()> {
        match state.resizer.flush_remaining()? {
            ComputedBatches::Single(batch) => self.insert_batch_agg_hash_table(state, batch)?,
            ComputedBatches::Multi(batches) => {
                // Technically shouldn't happen.
                for batch in batches {
                    self.insert_batch_agg_hash_table(state, batch)?;
                }
            }
            ComputedBatches::None => (), // We're good, no batches in the resizer.
        }

        Ok(())
    }

    /// Inserts a single batch into the partition-local aggregate hash table.
    fn insert_batch_agg_hash_table(
        &self,
        state: &mut AggregatingPartitionState,
        batch: Batch,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Columns that we're computing the aggregate over.
        let aggregate_columns: Vec<_> = self
            .aggregate_columns
            .iter()
            .map(|idx| batch.column(*idx).expect("aggregate input column to exist"))
            .collect();

        // Get the columns containg the "group" values (the columns in a
        // a GROUP BY).
        let grouping_columns: Vec<_> = self
            .group_columns
            .iter()
            .map(|idx| batch.column(*idx).expect("grouping column to exist"))
            .collect();

        let num_rows = batch.num_rows();
        state.hash_buf.resize(num_rows, 0);
        state.partitions_idx_buf.resize(num_rows, 0);

        let null_col = Array::new_untyped_null_array(num_rows);

        let mut masked_grouping_columns: Vec<&Array> = Vec::with_capacity(grouping_columns.len());

        // For null mask, create a new set of grouping values, hash
        // them, and put into the hash maps.
        for null_mask in &self.null_masks {
            masked_grouping_columns.clear();

            for (col_idx, col_is_null) in null_mask.iter().enumerate() {
                if col_is_null {
                    masked_grouping_columns.push(&null_col);
                } else {
                    masked_grouping_columns.push(grouping_columns[col_idx]);
                }
            }

            let group_id = null_mask.try_as_u64()?;

            // Compute hashes on the group by values.
            let hashes = HashExecutor::hash(&masked_grouping_columns, &mut state.hash_buf)?;

            // Compute _output_ partitions based on the hash values.
            let num_partitions = state.output_hashtables.len();
            for (partition, hash) in state.partitions_idx_buf.iter_mut().zip(hashes.iter()) {
                *partition = partition_for_hash(*hash, num_partitions);
            }

            // For each partition, produce a selection vector, and
            // insert the rows corresponding to that partition into the
            // partition's hash table.
            for (partition_idx, partition_hashtable) in
                state.output_hashtables.iter_mut().enumerate()
            {
                // Only select rows that this partition is concerned
                // about.
                let selection: SelectionVector = state
                    .partitions_idx_buf
                    .iter()
                    .enumerate()
                    .filter_map(|(row, selected_partition)| {
                        if selected_partition == &partition_idx {
                            Some(row)
                        } else {
                            None
                        }
                    })
                    .collect();

                partition_hashtable.insert_groups(
                    &masked_grouping_columns,
                    hashes,
                    &aggregate_columns,
                    &selection,
                    group_id,
                )?;
            }
        }

        Ok(())
    }
}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        // TODO: grouping sets
        ExplainEntry::new("HashAggregate").with_values("aggregate_columns", &self.aggregate_columns)
    }
}
