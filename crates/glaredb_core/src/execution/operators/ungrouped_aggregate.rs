use std::fmt::Debug;
use std::task::Context;

use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::buffer::buffer_manager::NopBufferManager;
use crate::buffer::typed::AlignedBuffer;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalAggregateExpression;

#[derive(Debug)]
pub enum UngroupedAggregatePartitionState {
    /// Partition is aggregating.
    Aggregating {
        /// Binary data containing values for each aggregate.
        ///
        /// This will be aligned and sized according to the aggregate layout.
        values: AlignedBuffer<u8>,
        /// Reusable buffer for storing pointers to an aggregate state.
        ptr_buf: Vec<*mut u8>,
        /// Aggregate inputs buffer.
        agg_inputs: Batch,
    },
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

#[derive(Debug)]
pub struct UngroupedAggregateOperatorState {
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
struct OperatorStateInner {
    /// Remaining number of partitions we're waiting to complete before
    /// producing the final values.
    ///
    /// Initialize to number of partitions.
    remaining: usize,
    /// Values combined from all partitions.
    values: AlignedBuffer<u8>,
}

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate {
    /// Aggregate layout.
    ///
    /// This will have no groups associated with it.
    layout: AggregateLayout,
    /// Output types for the aggregates.
    output_types: Vec<DataType>,
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

        PhysicalUngroupedAggregate {
            layout,
            output_types,
        }
    }

    /// Initalizes a new buffer for the aggregates in this operator.
    fn try_init_buffer(&self) -> Result<AlignedBuffer<u8>> {
        let values = AlignedBuffer::<u8>::try_with_capacity_and_alignment(
            &NopBufferManager,
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

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(UngroupedAggregateOperatorState {
            inner: Mutex::new(OperatorStateInner {
                remaining: 0,
                values: self.try_init_buffer()?,
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

        let states = (0..partitions)
            .map(|_| {
                Ok(UngroupedAggregatePartitionState::Aggregating {
                    values: self.try_init_buffer()?,
                    ptr_buf: Vec::with_capacity(props.batch_size),
                    agg_inputs: Batch::new(agg_input_types.clone(), 0)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut inner = operator_state.inner.lock();
        inner.remaining = partitions;

        Ok(states)
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        match state {
            UngroupedAggregatePartitionState::Aggregating {
                values,
                ptr_buf,
                agg_inputs,
            } => {
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

                // All inputs update the same "group".
                ptr_buf.clear();
                ptr_buf.extend(std::iter::repeat(values.as_mut_ptr()).take(input.num_rows));

                // SAFETY: The aggregate values buffer should have been
                // allocated according to this layout.
                unsafe {
                    self.layout.update_states(
                        ptr_buf.as_mut_slice(),
                        &agg_inputs.arrays,
                        input.num_rows,
                    )?;
                }

                Ok(PollExecute::NeedsMore)
            }
            UngroupedAggregatePartitionState::Draining => {
                output.reset_for_write()?;
                let operator_state = operator_state.inner.lock();

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
            UngroupedAggregatePartitionState::Aggregating { values, .. } => {
                let mut op_state = operator_state.inner.lock();

                let src_ptr = values.as_mut_ptr();
                let dest_ptr = op_state.values.as_mut_ptr();

                // No groups, so we're just combining single states (slices of
                // len 1).
                //
                // SAFETY: Both src and dest pointers should point to valid
                // states according to the layout.
                unsafe {
                    self.layout
                        .combine_states(&mut [src_ptr], &mut [dest_ptr])?
                }

                op_state.remaining -= 1;

                if op_state.remaining == 0 {
                    // This is the final partition to complete, it'll be the one
                    // that'll drain the final values.
                    *state = UngroupedAggregatePartitionState::Draining;
                    Ok(PollFinalize::NeedsDrain)
                } else {
                    // Other partitions still need to complete. This partition
                    // will never produce output.
                    *state = UngroupedAggregatePartitionState::Finished;
                    Ok(PollFinalize::Finalized)
                }
            }
            _ => Err(DbError::new("Ungrouped aggregate state in invalid state")),
        }
    }
}

impl Explainable for PhysicalUngroupedAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
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
