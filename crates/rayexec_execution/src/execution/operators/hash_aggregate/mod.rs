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
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{ExecuteInOutState, PollExecute, PollFinalize, UnaryInputStates};
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::physical_type::PhysicalU64;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::OutBuffer;
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
    /// Return type for the aggregate.
    pub datatype: DataType,
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
    /// Partition is currently aggregate local inputs.
    LocalAggregating(LocalAggregatingState),
    /// Partition is waiting for all other partitions to complete their local
    /// aggregation.
    PendingProduce(PendingProduceState),
    /// Partition is producing output.
    Producing(ProducingState),
}

#[derive(Debug)]
struct LocalAggregatingState {
    /// Index of this partition.
    partition_idx: usize,
    /// Local aggregate tables for each partition.
    partition_tables: Vec<HashTable>,
    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
    /// Reusable buffers for storing which hash table a row should be inserting
    /// into.
    ///
    /// These are selection vectors on the input batch.
    partitioning_bufs: Vec<Vec<usize>>,
}

#[derive(Debug)]
struct PendingProduceState {
    partitions_idx: usize,
}

#[derive(Debug)]
struct ProducingState {
    /// Drain for this partition's hash table.
    drain: HashTableDrain,
}

#[derive(Debug)]
pub struct HashAggregateOperatorState {
    inner: Mutex<HashAggregateOperatoreStateInner>,
}

#[derive(Debug)]
struct HashAggregateOperatoreStateInner {
    /// Completed hash table for each partition.
    ///
    /// Indexed by partition idx.
    ///
    /// Once all inputs are finished, each partitions takes its assigned vec of
    /// tables, combines them into one, then begins draining.
    tables: Vec<Vec<HashTable>>,
    /// Remaining inputs.
    ///
    /// Initialized to number of partitions.
    remaining: usize,
    /// Wakers for partititions waiting for other partitions to complete.
    ///
    /// Indexed by partition idx.
    ///
    /// Wakers are all woken when `remaining` reaches zero.
    wakers: Vec<Option<Waker>>,
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
    /// Aggregate expressions.
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
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::LocalAggregating(state) => {
                // Insert into local hash tables.
                let input = inout.input.required("input batch required")?;
                if input.num_rows() == 0 {
                    return Ok(PollExecute::NeedsMore);
                }

                // Columns that we're computing the aggregate over.
                let aggregate_columns: Vec<_> = self
                    .aggregate_columns
                    .iter()
                    .map(|idx| {
                        input
                            .array(*idx)
                            .expect("aggregate input column to exist")
                            .clone()
                    }) // TODO
                    .collect();

                // Get the columns containg the "group" values (the columns in a
                // a GROUP BY).
                let grouping_columns: Vec<_> = self
                    .group_columns
                    .iter()
                    .map(|idx| input.array(*idx).expect("grouping column to exist"))
                    .collect();

                let num_rows = input.num_rows();
                state.hash_buf.resize(num_rows, 0);

                let mut masked_grouping_columns: Vec<Array> =
                    Vec::with_capacity(grouping_columns.len());

                // For null mask, create a new set of grouping values, hash
                // them, and put into the hash maps.
                for null_mask in &self.null_masks {
                    masked_grouping_columns.clear();

                    for (col_idx, col_is_null) in null_mask.iter().enumerate() {
                        if col_is_null {
                            // Create column with all nulls but retain the datatype.
                            let null_col = Array::try_new_typed_null(
                                &NopBufferManager,
                                grouping_columns[col_idx].datatype().clone(),
                                num_rows,
                            )?;
                            masked_grouping_columns.push(null_col);
                        } else {
                            // TODO: Take reference.
                            unimplemented!()
                            // masked_grouping_columns.push(grouping_columns[col_idx].clone());
                        }
                    }

                    // Group id for disambiguating NULL values in user columns
                    // vs NULLs we're applying for the mask.
                    let grouping_set_id = null_mask.try_as_u64()?;

                    // Append group id to group val columns. Can be retrieved via the
                    // GROUPING function call.
                    masked_grouping_columns
                        .push(ScalarValue::UInt64(grouping_set_id).as_array(num_rows)?);
                }

                // Compute hashes on the group by values.
                //
                // Note we hash the masked columns and the grouping set id
                // columns since the grouping set is essentially part of what
                // we're comparing. We can reduce collisions by including that
                // column in the hash.
                state.hash_buf.resize(num_rows, 0);
                hash_many_arrays(
                    &masked_grouping_columns,
                    input.selection(),
                    &mut state.hash_buf,
                )?;

                state
                    .partitioning_bufs
                    .iter_mut()
                    .for_each(|sel| sel.clear());

                // Compute selections for each partition based on hash.
                for (row_idx, &hash) in state.hash_buf.iter().enumerate() {
                    let part_idx = hash as usize % state.partitioning_bufs.len();
                    state.partitioning_bufs[part_idx].push(row_idx);
                }

                // TODO: Try to not do this.
                let mut part_hashes = Vec::new();

                for (part_idx, hashtable) in state.partition_tables.iter_mut().enumerate() {
                    let selection = &state.partitioning_bufs[part_idx];

                    // Select hashes we're inserting for this partition.
                    part_hashes.clear();
                    part_hashes.extend(selection.iter().map(|&sel| state.hash_buf[sel]));
                }

                unimplemented!()
            }
            HashAggregatePartitionState::PendingProduce(state) => {
                unimplemented!()
            }
            HashAggregatePartitionState::Producing(state) => {
                unimplemented!()
            }
        }
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::LocalAggregating(state) => {
                // Inputs are finished, write to global state.
                unimplemented!()
            }
            _ => Err(RayexecError::new(
                "Finalize called more than once for hash aggregate",
            )),
        }
    }
}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        // TODO: grouping sets
        ExplainEntry::new("HashAggregate").with_values("aggregate_columns", &self.aggregate_columns)
    }
}
