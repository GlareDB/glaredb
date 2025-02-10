use std::fmt::Debug;
use std::task::Context;

use parking_lot::Mutex;
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{
    ExecutableOperator,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::raw::AlignedBuffer;
use crate::arrays::row::aggregate_layout::{AggregateInfo, AggregateLayout};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalAggregateExpression;

#[derive(Debug)]
pub enum UngroupedAggregatePartitionState {
    /// Partition is aggregating.
    Aggregating {
        /// Binary data containing values for each aggregate.
        ///
        /// This will be aligned and sized according to the aggregate layout.
        values: AlignedBuffer<u8, NopBufferManager>,
        /// Reusable buffer for storing pointers to an aggregate state.
        ptr_buf: Vec<*mut u8>,
    },
    /// Partition is draining.
    ///
    /// Only a single partition should drain.
    Draining,
    /// Partition is finished, no more output will be produced.
    Finished,
}

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
    values: AlignedBuffer<u8, NopBufferManager>,
}

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate {
    /// Aggregates we're computing.
    ///
    /// Used to create the initial states.
    aggregates: Vec<PhysicalAggregateExpression>,
    /// Aggregate layout.
    ///
    /// This will have no groups associated with it.
    layout: AggregateLayout,
}

impl PhysicalUngroupedAggregate {
    pub fn new(aggregates: Vec<PhysicalAggregateExpression>) -> Self {
        let infos = aggregates.iter().map(|agg| AggregateInfo {
            align: agg.function.function_impl.state_align,
            size: agg.function.function_impl.state_size,
        });

        let layout = AggregateLayout::new([], infos);

        PhysicalUngroupedAggregate { aggregates, layout }
    }

    /// Intializes a new buffer for the aggregates in this operator.
    fn try_init_buffer(&self) -> Result<AlignedBuffer<u8, NopBufferManager>> {
        let values = AlignedBuffer::<u8, _>::try_with_capacity_and_alignment(
            &NopBufferManager,
            self.layout.row_width,
            self.layout.base_align,
        )?;

        for (agg_idx, agg) in self.aggregates.iter().enumerate() {
            let extra = agg.function.function_impl.extra.as_deref().map(|v| v as _);
            // SAFETY: Buffer allocated according to the layout width and
            // alignement. The state pointer should be correctly aligned.
            unsafe {
                let state_ptr = values
                    .as_mut_ptr()
                    .byte_add(self.layout.aggregate_offsets[agg_idx]);

                (agg.function.function_impl.init_fn)(extra, state_ptr)
            }
        }

        Ok(values)
    }
}

impl ExecutableOperator for PhysicalUngroupedAggregate {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Self::States> {
        let partition_states = (0..partitions)
            .map(|_| {
                Ok(PartitionState::UngroupedAggregate(
                    UngroupedAggregatePartitionState::Aggregating {
                        values: self.try_init_buffer()?,
                        ptr_buf: Vec::with_capacity(batch_size),
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let operator_state = OperatorState::UngroupedAggregate(UngroupedAggregateOperatorState {
            inner: Mutex::new(OperatorStateInner {
                remaining: partitions,
                values: self.try_init_buffer()?,
            }),
        });

        Ok(UnaryInputStates {
            operator_state,
            partition_states,
        })
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: super::ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::UngroupedAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            UngroupedAggregatePartitionState::Aggregating { values, ptr_buf } => {
                let input = inout.input.required("input batch required")?;

                for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                    // TODO: Avoid allocating every time.
                    let cols: Vec<_> = agg
                        .columns
                        .iter()
                        .map(|col| &input.arrays[col.idx])
                        .collect();

                    // SAFETY: The aggregate values buffer should have been
                    // allocated according to this layout. The layout offsets
                    // should then be in bounds.
                    let state_ptr = unsafe {
                        values
                            .as_mut_ptr()
                            .byte_add(self.layout.aggregate_offsets[agg_idx])
                    };

                    // All rows will be updating the same state.
                    ptr_buf.clear();
                    ptr_buf.extend(std::iter::repeat(state_ptr).take(input.num_rows));

                    let extra = agg.function.function_impl.extra.as_deref().map(|v| v as _);
                    // SAFETY: Values buffer should have been aligned to a base
                    // alignement which should be a multiple of this aggregate's
                    // alignment. The computed offset should then result in an
                    // aligned pointer.
                    unsafe {
                        (agg.function.function_impl.update_fn)(
                            extra,
                            &cols,
                            input.num_rows,
                            ptr_buf.as_mut_slice(),
                        )?;
                    }
                }

                Ok(PollExecute::NeedsMore)
            }
            UngroupedAggregatePartitionState::Draining => {
                let op_state = match operator_state {
                    OperatorState::UngroupedAggregate(state) => state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                let output = inout.output.required("output batch required")?;
                output.reset_for_write()?;

                for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                    let arr = &mut output.arrays[agg_idx];

                    let extra = agg.function.function_impl.extra.as_deref().map(|v| v as _);
                    unsafe {
                        let state_ptr = op_state
                            .values
                            .as_mut_ptr()
                            .byte_add(self.layout.aggregate_offsets[agg_idx]);

                        (agg.function.function_impl.finalize_fn)(extra, &mut [state_ptr], arr)?
                    }
                }
                output.set_num_rows(1)?;

                *state = UngroupedAggregatePartitionState::Finished;

                Ok(PollExecute::Exhausted)
            }
            UngroupedAggregatePartitionState::Finished => {
                let output = inout.output.required("output batch required")?;
                output.set_num_rows(0)?;

                Ok(PollExecute::Exhausted)
            }
        }
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::UngroupedAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let op_state = match operator_state {
            OperatorState::UngroupedAggregate(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            UngroupedAggregatePartitionState::Aggregating { values, .. } => {
                let mut op_state = op_state.inner.lock();

                for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                    unsafe {
                        let src_ptr = values
                            .as_mut_ptr()
                            .byte_add(self.layout.aggregate_offsets[agg_idx]);

                        let dest_ptr = op_state
                            .values
                            .as_mut_ptr()
                            .byte_add(self.layout.aggregate_offsets[agg_idx]);

                        let extra = agg.function.function_impl.extra.as_deref().map(|v| v as _);
                        // No groups, so we're just combining single states
                        // (slices of len 1).
                        //
                        // SAFETY: Both src and dest pointers should point to
                        // valid states according to the layout.
                        (agg.function.function_impl.combine_fn)(
                            extra,
                            &mut [src_ptr],
                            &mut [dest_ptr],
                        )?;
                    };
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
                    Ok(PollFinalize::Finalized)
                }
            }
            _ => Err(RayexecError::new(
                "Ungrouped aggregate state in invalid state",
            )),
        }
    }
}

impl Explainable for PhysicalUngroupedAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalUngroupedAggregate")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::{assert_batches_eq, generate_batch};
    use crate::execution::operators::testutil::{plan_aggregate, OperatorWrapper};
    use crate::functions::aggregate::builtin::sum;

    #[test]
    fn single_aggregate_single_partition() {
        let agg = plan_aggregate(&sum::Sum, [DataType::Int64]);
        let mut operator = OperatorWrapper::new(PhysicalUngroupedAggregate::new(vec![agg]));

        let mut states = operator.create_unary_states(1024, 1);

        let mut input = generate_batch!([1_i64, 2, 3, 4]);
        let mut output = Batch::try_new([DataType::Int64], 1024).unwrap();

        let poll = operator.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = operator.unary_finalize(&mut states, 0);
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = operator.unary_execute_out(&mut states, 0, &mut output);
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!([10_i64]);
        assert_batches_eq(&expected, &output);
    }
}
