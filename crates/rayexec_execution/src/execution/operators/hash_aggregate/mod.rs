pub mod chunk;
pub mod compare;
pub mod distinct;
pub mod drain;
pub mod entry;
pub mod hash_table;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::task::{Context, Waker};

use distinct::DistinctGroupedStates;
use drain::HashTableDrain;
use hash_table::HashTable;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use super::{ExecutionStates, InputOutputStates, PollFinalize};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::array::physical_type::PhysicalU64;
use crate::arrays::executor::scalar::{HashExecutor, UnaryExecutor};
use crate::arrays::scalar::ScalarValue;
use crate::arrays::selection::SelectionVector;
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
use crate::functions::aggregate::states::AggregateGroupStates;
use crate::functions::aggregate::AggregateFunctionImpl;
use crate::logical::logical_aggregate::GroupingFunction;

#[derive(Debug)]
pub struct Aggregate {
    /// Function for producing the aggregate state.
    pub function: Box<dyn AggregateFunctionImpl>,
    /// Columns that will be inputs into the aggregate.
    pub col_selection: Bitmap,
    /// If inputs are distinct.
    pub is_distinct: bool,
}

impl Aggregate {
    pub fn new_states(&self) -> Result<AggregateStates> {
        if self.is_distinct {
            let states = Box::new(DistinctGroupedStates::new(self.function.new_states()));
            Ok(AggregateStates {
                states,
                col_selection: self.col_selection.clone(),
            })
        } else {
            Ok(AggregateStates {
                states: self.function.new_states(),
                col_selection: self.col_selection.clone(),
            })
        }
    }
}

/// States for a single aggregation.
#[derive(Debug)]
pub struct AggregateStates {
    /// The states we're tracking for a single aggregate.
    ///
    /// Internally the state are stored in a vector, with the index of the
    /// vector corresponding to the index of the group in the table's
    /// `group_values` vector.
    pub states: Box<dyn AggregateGroupStates>,

    /// Bitmap for selecting columns from the input to the hash map.
    ///
    /// This is used to allow the hash map to handle states for different
    /// aggregates working on different columns. For example:
    ///
    /// SELECT SUM(a), MIN(b) FROM ...
    ///
    /// This query computes aggregates on columns 'a' and 'b', but to minimize
    /// work, we pass both 'a' and 'b' to the hash table in one pass. Then this
    /// bitmap is used to further refine the inputs specific to the aggregate.
    pub col_selection: Bitmap,
}

#[derive(Debug)]
pub enum HashAggregatePartitionState {
    /// Partition is currently aggregating inputs.
    Aggregating(AggregatingPartitionState),
    /// Partition is currently producing final aggregate results.
    Producing(ProducingPartitionState),
}

#[derive(Debug)]
pub struct AggregatingPartitionState {
    /// Index of this partition.
    partition_idx: usize,
    /// Output hash tables for storing aggregate states.
    ///
    /// There exists one hash table per output partition.
    output_hashtables: Vec<HashTable>,
    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
    /// Resusable partitions buffer.
    partitions_idx_buf: Vec<usize>,
    partition_row_sel: Vec<SelectionVector>,
}

#[derive(Debug)]
pub struct ProducingPartitionState {
    /// Index of this partition.
    partition_idx: usize,
    /// The aggregate hash table that we're pulling results from.
    ///
    /// May be None if the final hash table hasn't been built yet. If it
    /// hasn't been built, then the shared state will be need to be checked.
    hashtable_drain: Option<HashTableDrain>,
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
    completed: Vec<HashTable>,

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
    /// Grouping functions that we should compute at the end.
    grouping_functions: Vec<GroupingFunction>,
    /// Null masks for determining which column values should be part of a
    /// group.
    null_masks: Vec<Bitmap>,
    /// Distinct columns that are used in the grouping sets.
    group_columns: Vec<usize>,
    /// Union of all column indices that are inputs to the aggregate functions.
    aggregate_columns: Vec<usize>,
    exprs: Vec<PhysicalAggregateExpression>,
}

impl PhysicalHashAggregate {
    pub fn new(
        exprs: Vec<PhysicalAggregateExpression>,
        grouping_sets: Vec<BTreeSet<usize>>,
        grouping_functions: Vec<GroupingFunction>,
    ) -> Self {
        // Collect all unique column indices that are part of computing the
        // aggregate.
        let mut agg_input_cols = BTreeSet::new();
        for expr in &exprs {
            agg_input_cols.extend(expr.columns.iter().map(|expr| expr.idx));
        }

        // Used to generate intial null masks. This doesn't take into account
        // the physical column offset.
        let mut distinct_group_cols: BTreeSet<usize> = BTreeSet::new();
        for set in &grouping_sets {
            distinct_group_cols.extend(set.iter());
        }

        let null_masks = grouping_sets
            .iter()
            .map(|set| {
                let mut mask = Bitmap::new_with_all_true(distinct_group_cols.len());
                for &col_idx in set {
                    mask.set_unchecked(col_idx, false);
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
            grouping_functions,
            null_masks,
            group_columns,
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
                    let aggregates: Vec<_> = self
                        .exprs
                        .iter()
                        .zip(col_selections.iter())
                        .map(|(expr, col_selection)| Aggregate {
                            function: expr.function.function_impl.clone(),
                            col_selection: col_selection.clone(),
                            is_distinct: expr.is_distinct,
                        })
                        .collect();
                    HashTable::new(16, aggregates)
                })
                .collect();

            let partition_state = PartitionState::HashAggregate(
                HashAggregatePartitionState::Aggregating(AggregatingPartitionState {
                    partition_idx: idx,
                    output_hashtables: partition_local_tables,
                    hash_buf: Vec::new(),
                    partitions_idx_buf: Vec::new(),
                    partition_row_sel: (0..num_partitions)
                        .map(|_| SelectionVector::empty())
                        .collect(),
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
                self.insert_batch_agg_hash_table(state, batch)?;

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
            HashAggregatePartitionState::Aggregating(_) => {
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
                    let mut completed = std::mem::take(&mut shared_state.completed);
                    let mut final_table =
                        completed.pop().expect("there to be at least one partition");

                    final_table.merge_many(&mut completed)?;

                    let drain = final_table.into_drain();
                    state.hashtable_drain = Some(drain);
                }

                // Drain should be Some by here.
                let batch = match state.hashtable_drain.as_mut().unwrap().next() {
                    Some(Ok(batch)) => batch,
                    Some(Err(e)) => return Err(e),
                    None => return Ok(PollPull::Exhausted),
                };

                // Prune off GROUP ID column, generate appropriate GROUPING
                // outputs.
                let mut arrays = batch.into_arrays();
                let group_ids = arrays
                    .pop()
                    .ok_or_else(|| RayexecError::new("Missing group ids arrays"))?;

                // TODO: This can be pre-computed, we don't need to compute this
                // on the output.
                for grouping_function in &self.grouping_functions {
                    let builder = ArrayBuilder {
                        datatype: DataType::UInt64,
                        buffer: PrimitiveBuffer::with_len(group_ids.logical_len()),
                    };

                    let array = UnaryExecutor::execute::<PhysicalU64, _, _>(
                        &group_ids,
                        builder,
                        |id, buf| {
                            // Compute the output for GROUPING.
                            let mut v: u64 = 0;

                            // Reverse iter to match postgres, the right-most
                            // column the GROUPING corresponds to the least
                            // significant bit.
                            for (idx, &group_col) in
                                grouping_function.group_exprs.iter().rev().enumerate()
                            {
                                let col_bit = 1 << group_col;
                                if id & col_bit != 0 {
                                    v |= 1 << idx;
                                }
                            }

                            buf.put(&v);
                        },
                    )?;

                    arrays.push(array);
                }

                let batch = Batch::try_new(arrays)?;

                Ok(PollPull::Computed(ComputedBatches::Single(batch)))
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
            .map(|idx| {
                batch
                    .column(*idx)
                    .expect("aggregate input column to exist")
                    .clone()
            }) // TODO
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

        let mut masked_grouping_columns: Vec<Array> = Vec::with_capacity(grouping_columns.len());

        // Reused to select hashes per partition.
        let mut partition_hashes = Vec::new();

        // For null mask, create a new set of grouping values, hash
        // them, and put into the hash maps.
        for null_mask in &self.null_masks {
            masked_grouping_columns.clear();

            for (col_idx, col_is_null) in null_mask.iter().enumerate() {
                if col_is_null {
                    // Create column with all nulls but retain the datatype.
                    let null_col = Array::new_typed_null_array(
                        grouping_columns[col_idx].datatype().clone(),
                        num_rows,
                    )?;
                    masked_grouping_columns.push(null_col);
                } else {
                    masked_grouping_columns.push(grouping_columns[col_idx].clone());
                }
            }

            // Group id for disambiguating NULL values in user columns vs NULLs
            // we're applying for the mask.
            let grouping_set_id = null_mask.try_as_u64()?;

            // Append group id to group val columns. Can be retrieved via the
            // GROUPING function call.
            masked_grouping_columns.push(ScalarValue::UInt64(grouping_set_id).as_array(num_rows)?);

            // Compute hashes on the group by values.
            //
            // Note we hash the masked columns and the grouping set id columns
            // since the grouping set is essentially part of what we're
            // comparing. We can reduce collisions by including that column in
            // the hash.
            let hashes = HashExecutor::hash_many(&masked_grouping_columns, &mut state.hash_buf)?;

            // Compute _output_ partitions based on the hash values.
            let num_partitions = state.output_hashtables.len();

            state
                .partition_row_sel
                .iter_mut()
                .for_each(|sel| sel.clear());

            for (row_idx, hash) in hashes.iter().enumerate() {
                let partition_idx = partition_for_hash(*hash, num_partitions);
                state.partition_row_sel[partition_idx].push_location(row_idx);
            }

            // For each partition, produce a selection vector, and
            // insert the rows corresponding to that partition into the
            // partition's hash table.
            for (partition_idx, partition_hashtable) in
                state.output_hashtables.iter_mut().enumerate()
            {
                // Only select rows that this partition is concerned
                // about.
                let selection = Arc::new(state.partition_row_sel[partition_idx].clone());

                if selection.is_empty() {
                    // No group values from input is going into this partition.
                    continue;
                }

                // TODO: Try not to do this.
                partition_hashes.clear();
                partition_hashes.extend(selection.iter_locations().map(|loc| hashes[loc]));

                // Select agg inputs.
                let inputs: Vec<_> = aggregate_columns
                    .iter()
                    .map(|arr| {
                        let mut arr = arr.clone();
                        arr.select_mut(selection.clone());
                        arr
                    })
                    .collect();

                // Select group values.
                let groups: Vec<_> = masked_grouping_columns
                    .iter()
                    .map(|arr| {
                        let mut arr = arr.clone();
                        arr.select_mut(selection.clone());
                        arr
                    })
                    .collect();

                partition_hashtable.insert(&groups, &partition_hashes, &inputs)?;
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
