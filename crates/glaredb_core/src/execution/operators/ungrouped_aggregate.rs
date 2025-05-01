use std::fmt::Debug;
use std::task::Context;

use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use super::hash_aggregate::distinct_aggregates::{
    AggregateSelection,
    DistinctAggregateInfo,
    DistinctCollection,
    DistinctCollectionOperatorState,
    DistinctCollectionPartitionState,
};
use super::util::delayed_count::DelayedPartitionCount;
use super::util::partition_wakers::PartitionWakers;
use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_layout::{
    AggregateLayout,
    AggregateUpdateSelector,
    CompleteInputSelector,
};
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::buffer::db_vec::DbVec;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalAggregateExpression;

#[derive(Debug)]
pub enum UngroupedAggregatePartitionState {
    /// Partition is aggregating.
    Aggregating {
        /// Inputs to all aggregates.
        agg_inputs: Batch,
        inner: AggregatingPartitionState,
    },
    /// Partition is merging all distinct tables.
    ///
    /// Only the last partition to complete flushing the tables should be the
    /// one to merge.
    MergingDistinct { inner: AggregatingPartitionState },
    /// Partition is scanning the distinct collection and writing them to the
    /// global aggregate state.
    AggregatingDistinct { inner: AggregatingPartitionState },
    /// Partition is draining.
    ///
    /// Only a single partition should drain.
    Draining,
    /// Partition is finished, no more output will be produced.
    Finished,
}

// SAFETY: The `Vec<*mut u8>` is just a buffer for storing row pointers.
unsafe impl Send for UngroupedAggregatePartitionState {}
unsafe impl Sync for UngroupedAggregatePartitionState {}

/// State that carries over between different phases of aggregating.
#[derive(Debug)]
pub struct AggregatingPartitionState {
    /// Index of this partition.
    partition_idx: usize,
    /// Binary data containing values for each aggregate.
    ///
    /// This will be aligned and sized according to the aggregate layout.
    values: DbVec<u8>,
    /// Reusable buffer for storing pointers to an aggregate state.
    ptr_buf: Vec<*mut u8>,
    /// Reusable buffer for computing the row selection.
    row_selection: Vec<usize>,
    /// State for distinct aggregates.
    distinct_state: DistinctCollectionPartitionState,
}

#[derive(Debug)]
pub struct UngroupedAggregateOperatorState {
    batch_size: usize,
    /// Distinct aggregate inputs.
    distinct_collection: DistinctCollection,
    /// State for distinct collection.
    distinct_collection_op_state: DistinctCollectionOperatorState,
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
struct OperatorStateInner {
    /// Remaining number of partitions we're waiting to complete before
    /// producing the final values for non-distinct aggregates.
    ///
    /// Initialized to number of partitions.
    remaining_normal: DelayedPartitionCount,
    /// Same, but for distinct.
    remaining_distinct: DelayedPartitionCount,
    /// Values combined from all partitions.
    ///
    /// Aligned to the base alignment of the aggregate layout.
    values: DbVec<u8>,
    /// If the merging of the distinct tables is complete.
    distinct_merge_complete: bool,
    /// Wakers for partitions waiting on the distinct merge to complete before
    /// scanning.
    pending_distinct: PartitionWakers,
}

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate {
    /// Aggregate layout.
    ///
    /// This will have no groups associated with it.
    layout: AggregateLayout,
    /// Output types for the aggregates.
    output_types: Vec<DataType>,
    /// Distinct/not distinct aggregate indices.
    agg_selection: AggregateSelection,
}

impl PhysicalUngroupedAggregate {
    pub fn new(aggregates: impl IntoIterator<Item = PhysicalAggregateExpression>) -> Self {
        let layout = AggregateLayout::new([], aggregates);
        debug_assert_eq!(0, layout.groups.row_width);

        let output_types: Vec<_> = layout
            .aggregates
            .iter()
            .map(|agg| agg.function.state.return_type.clone())
            .collect();

        let agg_selection = AggregateSelection::new(&layout.aggregates);

        PhysicalUngroupedAggregate {
            layout,
            output_types,
            agg_selection,
        }
    }

    /// Initalizes a new buffer for the aggregates in this operator.
    fn try_init_buffer(&self) -> Result<DbVec<u8>> {
        let mut values = DbVec::<u8>::new_uninit_with_align(
            &DefaultBufferManager,
            self.layout.row_width,
            self.layout.base_align,
        )?;

        for (agg_idx, agg) in self.layout.aggregates.iter().enumerate() {
            // SAFETY: Buffer allocated according to the layout width and
            // alignement. The state pointer should be correctly aligned.
            unsafe {
                let state_ptr = values
                    .as_mut_ptr()
                    .byte_add(self.layout.aggregate_offsets[agg_idx]);
                agg.function.call_new_aggregate_state(state_ptr);
            }
        }

        Ok(values)
    }
}

impl BaseOperator for PhysicalUngroupedAggregate {
    const OPERATOR_NAME: &str = "UngroupedAggregate";

    type OperatorState = UngroupedAggregateOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let distinct_collection =
            DistinctCollection::new(self.agg_selection.distinct.iter().map(|&idx| {
                DistinctAggregateInfo {
                    inputs: &self.layout.aggregates[idx].columns,
                    groups: &[],
                }
            }))?;
        let distinct_collection_op_state =
            distinct_collection.create_operator_state(props.batch_size)?;

        Ok(UngroupedAggregateOperatorState {
            batch_size: props.batch_size,
            distinct_collection,
            distinct_collection_op_state,
            inner: Mutex::new(OperatorStateInner {
                remaining_normal: DelayedPartitionCount::uninit(),
                remaining_distinct: DelayedPartitionCount::uninit(),
                values: self.try_init_buffer()?,
                distinct_merge_complete: false,
                pending_distinct: PartitionWakers::empty(),
            }),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl ExecuteOperator for PhysicalUngroupedAggregate {
    type PartitionExecuteState = UngroupedAggregatePartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let agg_input_types: Vec<_> = self
            .layout
            .aggregates
            .iter()
            .flat_map(|agg| agg.columns.iter().map(|col| col.datatype.clone()))
            .collect();

        let mut inner = operator_state.inner.lock();
        inner.remaining_normal.set(partitions)?;
        inner.remaining_distinct.set(partitions)?;
        inner.pending_distinct.init_for_partitions(partitions);

        let distinct_states = operator_state
            .distinct_collection
            .create_partition_states(&operator_state.distinct_collection_op_state, partitions)?;
        debug_assert_eq!(distinct_states.len(), partitions);

        let states = distinct_states
            .into_iter()
            .enumerate()
            .map(|(partition_idx, distinct_state)| {
                Ok(UngroupedAggregatePartitionState::Aggregating {
                    inner: AggregatingPartitionState {
                        partition_idx,
                        values: self.try_init_buffer()?,
                        ptr_buf: Vec::with_capacity(props.batch_size),
                        row_selection: Vec::with_capacity(props.batch_size),
                        distinct_state,
                    },
                    agg_inputs: Batch::new(agg_input_types.clone(), 0)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(states)
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        match state {
            UngroupedAggregatePartitionState::Aggregating { agg_inputs, inner } => {
                // Get aggregate inputs.
                for (dest_idx, src_idx) in self
                    .layout
                    .aggregates
                    .iter()
                    .flat_map(|agg| agg.columns.iter().map(|col| col.idx))
                    .enumerate()
                {
                    agg_inputs.clone_array_from(dest_idx, (input, src_idx))?;
                }
                // Num rows used by distinct table.
                agg_inputs.set_num_rows(input.num_rows())?;

                // All inputs update the same "group".
                inner.ptr_buf.clear();
                inner.ptr_buf.extend(std::iter::repeat_n(
                    inner.values.as_mut_ptr(),
                    input.num_rows,
                ));

                // Update DISTINCT aggregates. This insert into a hash table for
                // deduplication.
                operator_state
                    .distinct_collection
                    .insert(&mut inner.distinct_state, agg_inputs)?;

                // Update non-DISTINCT aggregates. Updates the aggregate values
                // directly.
                //
                // SAFETY: The aggregate values buffer should have been
                // allocated according to this layout.
                inner.row_selection.clear();
                inner.row_selection.extend(0..input.num_rows);
                unsafe {
                    self.layout.update_states(
                        inner.ptr_buf.as_mut_slice(),
                        CompleteInputSelector::with_selection(
                            &self.layout,
                            &self.agg_selection.non_distinct,
                            &agg_inputs.arrays,
                        ),
                        &inner.row_selection,
                    )?;
                }

                Ok(PollExecute::NeedsMore)
            }
            UngroupedAggregatePartitionState::MergingDistinct { .. } => {
                // If we're in this state, we are guaranteed to the be last
                // partition to insert into the tables.
                //
                // Do the final merging of the distinct tables.
                operator_state
                    .distinct_collection
                    .merge_flushed(&operator_state.distinct_collection_op_state)?;

                // Update our own state to AggregatingDistinct.
                //
                // TODO: I hate this pattern.
                match std::mem::replace(state, UngroupedAggregatePartitionState::Finished) {
                    UngroupedAggregatePartitionState::MergingDistinct { inner } => {
                        *state = UngroupedAggregatePartitionState::AggregatingDistinct { inner }
                    }
                    _ => unreachable!(),
                }

                // Now let all other partitions know the distinct table can be
                // scanned now.
                let mut op_state = operator_state.inner.lock();
                op_state.distinct_merge_complete = true;
                op_state.pending_distinct.wake_all();

                // We also want to scan, trigger a re-poll.
                output.set_num_rows(0)?;
                Ok(PollExecute::HasMore)
            }
            UngroupedAggregatePartitionState::AggregatingDistinct { inner } => {
                let mut op_state_inner = operator_state.inner.lock();
                if !op_state_inner.distinct_merge_complete {
                    // Distinct merging not complete. Come back later.
                    op_state_inner
                        .pending_distinct
                        .store(cx.waker(), inner.partition_idx);
                    return Ok(PollExecute::Pending);
                }
                std::mem::drop(op_state_inner);

                // We have all distinct values, start aggregating on them one by
                // one.
                for distinct_idx in 0..operator_state.distinct_collection.num_distinct_tables() {
                    // Create buffer to use to drain the table.
                    let mut batch = Batch::new(
                        operator_state
                            .distinct_collection
                            .iter_table_types(distinct_idx),
                        operator_state.batch_size,
                    )?;

                    loop {
                        batch.reset_for_write()?;
                        operator_state.distinct_collection.scan(
                            &operator_state.distinct_collection_op_state,
                            &mut inner.distinct_state,
                            distinct_idx,
                            &mut batch,
                        )?;

                        if batch.num_rows() == 0 {
                            // Move to next distinct input.
                            break;
                        }

                        // Update aggregate states for all aggregates depending
                        // on this distinct input.
                        inner.ptr_buf.clear();
                        inner.ptr_buf.extend(std::iter::repeat_n(
                            inner.values.as_mut_ptr(),
                            batch.num_rows,
                        ));

                        let agg_iter = operator_state
                            .distinct_collection
                            .aggregates_for_table(distinct_idx)
                            .iter()
                            .map(|&distinct_agg_idx| {
                                // Distinct table only knows about distinct
                                // aggregates. Map that index back to the full
                                // aggregate list.
                                let agg_idx = self.agg_selection.distinct[distinct_agg_idx];
                                AggregateUpdateSelector {
                                    aggregate_idx: agg_idx,
                                    inputs: &batch.arrays,
                                }
                            });

                        inner.row_selection.clear();
                        inner.row_selection.extend(0..batch.num_rows);
                        unsafe {
                            self.layout.update_states(
                                &mut inner.ptr_buf,
                                agg_iter,
                                &inner.row_selection,
                            )?;
                        }
                    }
                }

                // Merge our local state with the global state now.
                let mut op_state_inner = operator_state.inner.lock();
                let src_ptr = inner.values.as_mut_ptr();
                let dest_ptr = op_state_inner.values.as_mut_ptr();

                unsafe {
                    self.layout.combine_states(
                        self.agg_selection.distinct.iter().copied(),
                        &mut [src_ptr],
                        &mut [dest_ptr],
                    )?
                }

                op_state_inner.remaining_distinct.dec_by_one()?;

                if op_state_inner.remaining_distinct.current()? == 0 {
                    // We're the last partition to finish, we'll be responsible
                    // for draining.
                    *state = UngroupedAggregatePartitionState::Draining;
                    output.set_num_rows(0)?;

                    // Pull again.
                    Ok(PollExecute::HasMore)
                } else {
                    // This partition is finished.
                    *state = UngroupedAggregatePartitionState::Finished;
                    output.set_num_rows(0)?;

                    Ok(PollExecute::Exhausted)
                }
            }
            UngroupedAggregatePartitionState::Draining => {
                output.reset_for_write()?;
                let mut operator_state = operator_state.inner.lock();

                // SAFETY: The aggregate values buffer should have been
                // allocated according to this layout.
                unsafe {
                    // Only finalizing a single state.
                    self.layout.finalize_states(
                        &mut [operator_state.values.as_mut_ptr()],
                        &mut output.arrays,
                    )?
                }

                output.set_num_rows(1)?;

                *state = UngroupedAggregatePartitionState::Finished;

                Ok(PollExecute::Exhausted)
            }
            UngroupedAggregatePartitionState::Finished => {
                output.set_num_rows(0)?;
                Ok(PollExecute::Exhausted)
            }
        }
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        match state {
            UngroupedAggregatePartitionState::Aggregating { inner, .. } => {
                // Flush distinct tables.
                operator_state.distinct_collection.flush(
                    &operator_state.distinct_collection_op_state,
                    &mut inner.distinct_state,
                )?;

                let mut op_state = operator_state.inner.lock();

                // Normal aggregate merge.
                let src_ptr = inner.values.as_mut_ptr();
                let dest_ptr = op_state.values.as_mut_ptr();

                // No groups, so we're just combining single states (slices of
                // len 1).
                //
                // Only merge the non-distinct aggregates right now. We still
                // need to produce the inputs & results for distinct aggregates.
                //
                // SAFETY: Both src and dest pointers should point to valid
                // states according to the layout.
                unsafe {
                    self.layout.combine_states(
                        self.agg_selection.non_distinct.iter().copied(),
                        &mut [src_ptr],
                        &mut [dest_ptr],
                    )?
                }

                let remaining = op_state.remaining_normal.dec_by_one()?;

                if self.agg_selection.distinct.is_empty() {
                    // No distinct aggregates.
                    if remaining == 0 {
                        // This partition will drain.
                        *state = UngroupedAggregatePartitionState::Draining;
                        Ok(PollFinalize::NeedsDrain)
                    } else {
                        // This partition is finished.
                        *state = UngroupedAggregatePartitionState::Finished;
                        Ok(PollFinalize::Finalized)
                    }
                } else {
                    // We do have distinct aggregates. All partitions will take
                    // part in draining the distinct hash tables.
                    //
                    // Only the last partition to complete normal aggregating
                    // will do the merge though.

                    let aggregating_state =
                        std::mem::replace(state, UngroupedAggregatePartitionState::Finished);
                    match aggregating_state {
                        UngroupedAggregatePartitionState::Aggregating { inner, .. } => {
                            if remaining == 0 {
                                // We're the last, we'll do the drain.
                                *state = UngroupedAggregatePartitionState::MergingDistinct { inner }
                            } else {
                                // We're not the last. Just jump to the
                                // aggregating distinct state so we can register
                                // a waker.
                                *state =
                                    UngroupedAggregatePartitionState::AggregatingDistinct { inner }
                            }
                        }
                        _ => unreachable!(),
                    }

                    // Both state will try to drain.
                    //
                    // MergingDistinct will begin the merge.
                    //
                    // AggregatingDistinct will register a waker since the
                    // merged table isn't ready yet.
                    Ok(PollFinalize::NeedsDrain)
                }
            }
            _ => Err(DbError::new("Ungrouped aggregate state in invalid state")),
        }
    }
}

impl Explainable for PhysicalUngroupedAggregate {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new(Self::OPERATOR_NAME, conf)
            .with_values(
                "aggregates",
                self.layout.aggregates.iter().map(|agg| agg.function.name),
            )
            .build()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::minmax::FUNCTION_SET_MIN;
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn single_aggregate_single_partition() {
        // SUM(col0)

        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 0), DataType::Int64).into()],
        )
        .unwrap();
        let agg = PhysicalAggregateExpression::new(sum_agg, [(0, DataType::Int64)]);

        let wrapper = OperatorWrapper::new(PhysicalUngroupedAggregate::new(vec![agg]));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        let mut input = generate_batch!([1_i64, 2, 3, 4]);
        let mut output = Batch::new([DataType::Int64], 1024).unwrap();

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut states[0])
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!([10_i64]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn single_aggregate_two_partitions() {
        // SUM(col0) with two partitions

        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 0), DataType::Int64).into()],
        )
        .unwrap();
        let agg = PhysicalAggregateExpression::new(sum_agg, [(0, DataType::Int64)]);

        let wrapper = OperatorWrapper::new(PhysicalUngroupedAggregate::new(vec![agg]));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 2)
            .unwrap();

        let mut output = Batch::new([DataType::Int64], 1024).unwrap();

        // Execute and exhaust first partition.
        let mut input = generate_batch!([1_i64, 2, 3, 4]);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut states[0])
            .unwrap();
        assert_eq!(PollFinalize::Finalized, poll);
        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);
        assert_eq!(0, output.num_rows());

        // Execute second partition, this will drain the final results.
        let mut input = generate_batch!([5_i64, 6, 7, 8]);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[1], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut states[1])
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);
        let poll = wrapper
            .poll_execute(&op_state, &mut states[1], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!([36_i64]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn two_aggregates_single_partition() {
        // SUM(col1), MIN(col0)

        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();
        let min_agg = bind_aggregate_function(
            &FUNCTION_SET_MIN,
            vec![expr::column((0, 0), DataType::Int64).into()],
        )
        .unwrap();

        let aggs = [
            PhysicalAggregateExpression::new(sum_agg, [(1, DataType::Int64)]),
            PhysicalAggregateExpression::new(min_agg, [(0, DataType::Int64)]),
        ];

        let wrapper = OperatorWrapper::new(PhysicalUngroupedAggregate::new(aggs));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        let mut input = generate_batch!([22_i64, 33, 11, 44], [1_i64, 2, 3, 4]);
        let mut output = Batch::new([DataType::Int64, DataType::Int64], 1024).unwrap();

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut states[0])
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);
        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!([10_i64], [11_i64]);
        assert_batches_eq(&expected, &output);
    }
}
