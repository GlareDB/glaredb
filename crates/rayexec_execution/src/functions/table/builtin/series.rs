use std::task::Context;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalI64};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::field::{ColumnSchema, Field};
use crate::execution::operators::{ExecutionProperties, PollExecute, PollFinalize};
use crate::expr;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::execute::TableExecuteFunction;
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::functions::Signature;
use crate::logical::statistics::StatisticsValue;

pub const FUNCTION_SET_GENERATE_SERIES: TableFunctionSet = TableFunctionSet {
    name: "generate_series",
    aliases: &[],
    doc: Some(&Documentation{
        category: Category::Table,
        description: "Generate a series of values from 'start' to 'end' incrementing by a step of 1. 'start' and 'end' are both inclusive.",
        arguments: &["start", "end"],
        example: None,
    }),
    functions: &[
        // generate_series(start, stop)
        RawTableFunction::new_execute(&Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Table), &GenerateSeriesI64),
        // generate_series(start, stop, step)
        RawTableFunction::new_execute(&Signature::new(&[DataTypeId::Int64, DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Table), &GenerateSeriesI64),
    ],
};

#[derive(Debug, Default)]
pub struct GenerateSeriesI64PartitionState {
    /// Current params.
    params: Option<SeriesParams>,
    /// Row in the input we're currently working on.
    current_row: usize,
}

#[derive(Debug, Clone)]
struct SeriesParams {
    curr: i64,
    stop: i64,
    step: i64,
}

impl SeriesParams {
    fn try_new(input: &Batch, row: usize) -> Result<Self> {
        let start = input.arrays[0].get_value(row)?.try_as_i64()?;
        let stop = input.arrays[1].get_value(row)?.try_as_i64()?;
        let step = input.arrays[2].get_value(row)?.try_as_i64()?;

        if step == 0 {
            return Err(RayexecError::new("Step cannot be zero"));
        }

        Ok(SeriesParams {
            curr: start,
            stop,
            step,
        })
    }

    /// Generate the next set of rows using the current parameters.
    ///
    /// Returns the count of values written to `out`.
    fn generate_next(&mut self, out: &mut Array) -> Result<usize> {
        let mut out = PhysicalI64::get_addressable_mut(&mut out.data)?;

        let mut idx = 0;
        if self.curr <= self.stop && self.step > 0 {
            // Going up.
            while self.curr <= self.stop && idx < out.len() {
                out.put(idx, &self.curr);
                self.curr += self.step;
                idx += 1;
            }
        } else if self.curr >= self.stop && self.step < 0 {
            // Going down.
            while self.curr >= self.stop && idx < out.len() {
                out.put(idx, &self.curr);
                self.curr += self.step;
                idx += 1;
            }
        }

        if idx == 0 {
            // Nothing written.
            return Ok(0);
        }

        // Calculate the start value for the next iteration.
        let last = out.slice.get(idx - 1).expect("value to exist");
        self.curr = *last + self.step;

        Ok(idx)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GenerateSeriesI64;

impl TableExecuteFunction for GenerateSeriesI64 {
    type BindState = ();

    type OperatorState = ();
    type PartitionState = GenerateSeriesI64PartitionState;

    fn bind(
        &self,
        mut input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        if input.positional.len() == 2 {
            // Push constant step value.
            input.positional.push(expr::lit(1_i64).into());
        }

        Ok(TableFunctionBindState {
            state: (),
            input,
            schema: ColumnSchema::new([Field::new("generate_series", DataType::Int64, false)]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_execute_operator_state(
        _bind_state: &Self::BindState,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn create_execute_partition_states(
        _op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let states: Vec<_> = (0..partitions)
            .map(|_| GenerateSeriesI64PartitionState {
                params: None,
                current_row: 0,
            })
            .collect();

        Ok(states)
    }

    fn poll_execute(
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        loop {
            if state.params.is_none() {
                // Need to generate params from current row.
                if state.current_row >= input.num_rows() {
                    // Need a new batch.
                    state.current_row = 0;
                    return Ok(PollExecute::NeedsMore);
                }

                // Get params from the current row.
                let params = SeriesParams::try_new(input, state.current_row)?;
                state.params = Some(params);
            }

            let count = state
                .params
                .as_mut()
                .unwrap()
                .generate_next(&mut output.arrays[0])?;

            if count == 0 {
                // Next row.
                state.params = None;
                state.current_row += 1;
                continue;
            }

            output.set_num_rows(count)?;

            // Next poll should execute with the same input batch.
            return Ok(PollExecute::HasMore);
        }
    }

    fn poll_finalize_execute(
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionState,
    ) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}

#[cfg(test)]
mod tests {
    use crate::util::task::noop_context;

    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn generate_series_single_row() {
        let mut state = GenerateSeriesI64PartitionState::default();

        // generate_series(1, 5, 1)
        let mut input = generate_batch!([1_i64], [5_i64], [1_i64]);

        let mut output = Batch::new([DataType::Int64], 5).unwrap();
        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([1_i64, 2, 3, 4, 5]);
        assert_batches_eq(&expected, &output);

        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
    }

    #[test]
    fn generate_series_single_row_out_lacks_capacity() {
        // Same as single row test, just we poll with an output capacity that
        // requires multiple polls to get all output.
        let mut state = GenerateSeriesI64PartitionState::default();

        // generate_series(1, 5, 1)
        let mut input = generate_batch!([1_i64], [5_i64], [1_i64]);

        let mut output = Batch::new([DataType::Int64], 3).unwrap();
        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([1_i64, 2, 3]);
        assert_batches_eq(&expected, &output);

        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([4_i64, 5]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn generate_series_single_row_out_lacks_capacity_by_1() {
        // Test off by one...
        let mut state = GenerateSeriesI64PartitionState::default();

        // generate_series(1, 5, 1)
        let mut input = generate_batch!([1_i64], [5_i64], [1_i64]);

        let mut output = Batch::new([DataType::Int64], 4).unwrap();
        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([1_i64, 2, 3, 4]);
        assert_batches_eq(&expected, &output);

        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([5_i64]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn generate_series_multiple_rows() {
        let mut state = GenerateSeriesI64PartitionState::default();

        // generate_series(1, 5, 1)
        // generate_series(4, 8, 2)
        let mut input = generate_batch!([1_i64, 4], [5_i64, 8], [1_i64, 2]);

        let mut output = Batch::new([DataType::Int64], 5).unwrap();
        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([1_i64, 2, 3, 4, 5]);
        assert_batches_eq(&expected, &output);

        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([4_i64, 6, 8]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn generate_series_neverending_start_gt_stop() {
        let mut state = GenerateSeriesI64PartitionState::default();

        // generate_series(5, 1, 1)
        let mut input = generate_batch!([5_i64], [1_i64], [1_i64]);

        let mut output = Batch::new([DataType::Int64], 5).unwrap();
        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
        assert_eq!(0, output.num_rows());
    }

    #[test]
    fn generate_series_neverending_start_lt_stop() {
        let mut state = GenerateSeriesI64PartitionState::default();

        // generate_series(1, 5, -1)
        let mut input = generate_batch!([1_i64], [5_i64], [-1_i64]);

        let mut output = Batch::new([DataType::Int64], 5).unwrap();
        let poll = GenerateSeriesI64::poll_execute(
            &mut noop_context(),
            &(),
            &mut state,
            &mut input,
            &mut output,
        )
        .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
        assert_eq!(0, output.num_rows());
    }
}
