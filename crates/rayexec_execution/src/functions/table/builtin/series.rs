use std::collections::HashMap;
use std::task::Context;

use rayexec_error::{OptionExt, RayexecError, Result};

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalI64};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::field::{Field, Schema};
use crate::arrays::scalar::OwnedScalarValue;
use crate::execution::operators::{ExecuteInOutState, PollExecute, PollFinalize};
use crate::expr::{self, Expression};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::table::inout::{TableInOutFunction, TableInOutPartitionState};
use crate::functions::table::{
    InOutPlanner,
    PlannedTableFunction,
    TableFunction,
    TableFunctionImpl,
    TableFunctionPlanner,
};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args_one_of,
    FunctionInfo,
    Signature,
};
use crate::logical::binder::table_list::TableList;
use crate::logical::statistics::StatisticsValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerateSeries;

impl FunctionInfo for GenerateSeries {
    fn name(&self) -> &'static str {
        "generate_series"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                positional_args: &[DataTypeId::Int64, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Any,
                doc: Some(&Documentation{
                    category: Category::Table,
                    description: "Generate a series of values from 'start' to 'end' incrementing by a step of 1. 'start' and 'end' are both inclusive.",
                    arguments: &["start", "end"],
                    example: None,
                })
            },
            Signature {
                positional_args: &[DataTypeId::Int64, DataTypeId::Int64, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Any,
                doc: Some(&Documentation{
                    category: Category::Table,
                    description: "Generate a series of values from 'start' to 'end' incrementing by 'step'. 'start' and 'end' are both inclusive.",
                    arguments: &["start", "end", "step"],
                    example: None,
                })
            },
        ]
    }
}

impl TableFunction for GenerateSeries {
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::InOut(&GenerateSeriesInOutPlanner)
    }
}

#[derive(Debug, Clone)]
pub struct GenerateSeriesInOutPlanner;

impl InOutPlanner for GenerateSeriesInOutPlanner {
    fn plan(
        &self,
        table_list: &TableList,
        mut positional_inputs: Vec<Expression>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> Result<PlannedTableFunction> {
        plan_check_num_args_one_of(&GenerateSeries, &positional_inputs, [2, 3])?;
        if !named_inputs.is_empty() {
            return Err(RayexecError::new(format!(
                "'{}' does not accept named arguments",
                GenerateSeries.name()
            )));
        }

        let datatypes = positional_inputs
            .iter()
            .map(|expr| expr.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        for datatype in &datatypes {
            if datatype != &DataType::Int64 {
                return Err(invalid_input_types_error(&GenerateSeries, &datatypes));
            }
        }

        if positional_inputs.len() == 2 {
            // Add constant for the 'step' argument.
            positional_inputs.push(expr::lit(1_i64))
        }

        Ok(PlannedTableFunction {
            function: Box::new(GenerateSeries),
            positional_inputs,
            named_inputs,
            function_impl: TableFunctionImpl::InOut(Box::new(GenerateSeriesInOutImpl)),
            cardinality: StatisticsValue::Unknown,
            schema: Schema::new([Field::new("generate_series", DataType::Int64, false)]),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GenerateSeriesInOutImpl;

impl TableInOutFunction for GenerateSeriesInOutImpl {
    fn create_states(
        &self,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableInOutPartitionState>>> {
        let states: Vec<_> = (0..num_partitions)
            .map(|_| {
                Box::new(GenerateSeriesInOutPartitionState {
                    params: None,
                    current_row: 0,
                }) as _
            })
            .collect();

        Ok(states)
    }
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

        if start > stop && step > 0 {
            return Err(RayexecError::new(
                "Never-ending series, start larger than stop with positive step",
            )
            .with_field("start", start)
            .with_field("stop", stop)
            .with_field("step", step));
        }

        if start < stop && step < 0 {
            return Err(RayexecError::new(
                "Never-ending series, start smaller than stop with negative step",
            )
            .with_field("start", start)
            .with_field("stop", stop)
            .with_field("step", step));
        }

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
        if self.curr < self.stop && self.step > 0 {
            // Going up.
            while self.curr <= self.stop && idx < out.len() {
                out.put(idx, &self.curr);
                self.curr += self.step;
                idx += 1;
            }
        } else if self.curr > self.stop && self.step < 0 {
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

#[derive(Debug)]
pub struct GenerateSeriesInOutPartitionState {
    /// Current params.
    params: Option<SeriesParams>,
    /// Row in the input we're currently working on.
    current_row: usize,
}

impl TableInOutPartitionState for GenerateSeriesInOutPartitionState {
    fn poll_execute(&mut self, _cx: &mut Context, inout: ExecuteInOutState) -> Result<PollExecute> {
        let output = &mut inout.output.required("output batch required")?;
        let input = inout.input.required("input batch required")?;

        loop {
            if self.params.is_none() {
                // Need to generate params from current row.
                if self.current_row >= input.num_rows() {
                    // Need a new batch.
                    self.current_row = 0;
                    return Ok(PollExecute::NeedsMore);
                }

                // Get params from the current row.
                let params = SeriesParams::try_new(input, self.current_row)?;
                self.params = Some(params);
            }

            let count = self
                .params
                .as_mut()
                .unwrap()
                .generate_next(&mut output.arrays[0])?;

            if count == 0 {
                // Next row.
                self.params = None;
                self.current_row += 1;
                continue;
            }

            output.set_num_rows(count)?;

            // Next poll should execute with the same input batch.
            return Ok(PollExecute::HasMore);
        }
    }

    fn poll_finalize(&mut self, _cx: &mut Context) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}

#[cfg(test)]
mod tests {

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::testutil::assert_batches_eq;
    use crate::functions::table::inout::testutil::StateWrapper;

    #[test]
    fn generate_series_single_row() {
        let mut state = StateWrapper::new(
            GenerateSeriesInOutImpl
                .create_states(1)
                .unwrap()
                .pop()
                .unwrap(),
        );

        // generate_series(1, 5, 1)
        let mut input = Batch::try_from_arrays([
            Array::try_from_iter([1]).unwrap(),
            Array::try_from_iter([5]).unwrap(),
            Array::try_from_iter([1]).unwrap(),
        ])
        .unwrap();

        let mut output =
            Batch::try_from_arrays(
                [Array::try_new(&NopBufferManager, DataType::Int64, 5).unwrap()],
            )
            .unwrap();

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([1_i64, 2, 3, 4, 5]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
    }

    #[test]
    fn generate_series_single_row_out_lacks_capacity() {
        // Same as single row test, just we poll with an output capacity that
        // requires multiple polls to get all output.
        let mut state = StateWrapper::new(
            GenerateSeriesInOutImpl
                .create_states(1)
                .unwrap()
                .pop()
                .unwrap(),
        );

        // generate_series(1, 5, 1)
        let mut input = Batch::try_from_arrays([
            Array::try_from_iter([1]).unwrap(),
            Array::try_from_iter([5]).unwrap(),
            Array::try_from_iter([1]).unwrap(),
        ])
        .unwrap();

        let mut output =
            Batch::try_from_arrays(
                [Array::try_new(&NopBufferManager, DataType::Int64, 3).unwrap()],
            )
            .unwrap();

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([1_i64, 2, 3]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = Batch::try_from_arrays([Array::try_from_iter([4_i64, 5]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
    }

    #[test]
    fn generate_series_multiple_rows() {
        let mut state = StateWrapper::new(
            GenerateSeriesInOutImpl
                .create_states(1)
                .unwrap()
                .pop()
                .unwrap(),
        );

        // generate_series(1, 5, 1)
        // generate_series(4, 8, 2)
        let mut input = Batch::try_from_arrays([
            Array::try_from_iter([1, 4]).unwrap(),
            Array::try_from_iter([5, 8]).unwrap(),
            Array::try_from_iter([1, 2]).unwrap(),
        ])
        .unwrap();

        let mut output =
            Batch::try_from_arrays(
                [Array::try_new(&NopBufferManager, DataType::Int64, 5).unwrap()],
            )
            .unwrap();

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([1_i64, 2, 3, 4, 5]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([4_i64, 6, 8]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        let poll = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut output),
            })
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
    }

    #[test]
    fn generate_series_neverending_start_gt_stop() {
        let mut state = StateWrapper::new(
            GenerateSeriesInOutImpl
                .create_states(1)
                .unwrap()
                .pop()
                .unwrap(),
        );

        // generate_series(5, 1, 1)
        let mut input = Batch::try_from_arrays([
            Array::try_from_iter([5]).unwrap(),
            Array::try_from_iter([1]).unwrap(),
            Array::try_from_iter([1]).unwrap(),
        ])
        .unwrap();

        let mut out = Batch::try_new([DataType::Int32], 1024).unwrap();

        let _ = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut out),
            })
            .unwrap_err();
    }

    #[test]
    fn generate_series_neverending_start_lt_stop() {
        let mut state = StateWrapper::new(
            GenerateSeriesInOutImpl
                .create_states(1)
                .unwrap()
                .pop()
                .unwrap(),
        );

        // generate_series(1, 5, -1)
        let mut input = Batch::try_from_arrays([
            Array::try_from_iter([1]).unwrap(),
            Array::try_from_iter([5]).unwrap(),
            Array::try_from_iter([-1]).unwrap(),
        ])
        .unwrap();

        let mut out = Batch::try_new([DataType::Int32], 1024).unwrap();

        let _ = state
            .poll_execute(ExecuteInOutState {
                input: Some(&mut input),
                output: Some(&mut out),
            })
            .unwrap_err();
    }
}
