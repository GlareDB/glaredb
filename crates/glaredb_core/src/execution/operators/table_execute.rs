use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::batch::Batch;
use crate::arrays::cache::NopCache;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::functions::table::{
    AnyTableOperatorState,
    AnyTablePartitionState,
    PlannedTableFunction,
};

#[derive(Debug)]
pub struct TableExecuteOperatorState {
    function_op_state: AnyTableOperatorState,
}

#[derive(Debug)]
pub struct TableExecutePartitionState {
    function_state: AnyTablePartitionState,
    /// Batch for holding a single row from the input when we have additional
    /// projections.
    row_batch: Batch,
    /// Current row we're working on.
    ///
    /// Only used if projecting input.
    curr_row_idx: usize,
    /// If we should set up the next row.
    ///
    /// Only used if projecting input.
    needs_next_row: bool,
}

/// Executes table function on table inputs.
///
/// May be paired with additional projects, with the results being implicitly
/// (horizonstally) concatenated with the results of the function.
///
/// Enables queries like the following:
/// ```text
/// >> select * from (values (1, 3, 4)) v(a, b, c), generate_series(a, b);
/// ┌───────┬───────┬───────┬─────────────────┐
/// │ a     │ b     │ c     │ generate_series │
/// │ Int32 │ Int32 │ Int32 │ Int64           │
/// ├───────┼───────┼───────┼─────────────────┤
/// │     1 │     3 │     4 │               1 │
/// │     1 │     3 │     4 │               2 │
/// │     1 │     3 │     4 │               3 │
/// └───────┴───────┴───────┴─────────────────┘
/// ```
///
/// Table function inputs should be ordered at the beginning of the input batch.
///
/// Output: [FUNCTION_RESULTS, ADDITIONAL_PROJECTIONS]
#[derive(Debug)]
pub struct PhysicalTableExecute {
    /// The table function.
    pub(crate) function: PlannedTableFunction,
    /// Optional projections.
    pub(crate) additional_projections: Vec<PhysicalColumnExpr>,
    /// Output types, including the the results of the table function along with
    /// the additional projections.
    pub(crate) output_types: Vec<DataType>,
    /// Input types for the input into this operator.
    ///
    /// Used to create a row batch if we we have additional projections.
    pub(crate) input_types: Vec<DataType>,
}

impl PhysicalTableExecute {
    pub fn new<P>(
        function: PlannedTableFunction,
        additional_projections: impl IntoIterator<Item = P>,
        input_types: impl IntoIterator<Item = DataType>,
    ) -> Self
    where
        P: Into<PhysicalColumnExpr>,
    {
        let additional_projections: Vec<_> = additional_projections
            .into_iter()
            .map(|proj| proj.into())
            .collect();

        let mut output_types: Vec<_> = function
            .bind_state
            .schema
            .fields
            .iter()
            .map(|f| f.datatype.clone())
            .collect();

        output_types.extend(additional_projections.iter().map(|p| p.datatype()));

        PhysicalTableExecute {
            function,
            additional_projections,
            output_types,
            input_types: input_types.into_iter().collect(),
        }
    }
}

impl BaseOperator for PhysicalTableExecute {
    const OPERATOR_NAME: &str = "TableExecute";

    type OperatorState = TableExecuteOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let op_state = self
            .function
            .raw
            .call_create_execute_operator_state(&self.function.bind_state, props)?;

        Ok(TableExecuteOperatorState {
            function_op_state: op_state,
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl ExecuteOperator for PhysicalTableExecute {
    type PartitionExecuteState = TableExecutePartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let states = self.function.raw.call_create_execute_partition_states(
            &operator_state.function_op_state,
            props,
            partitions,
        )?;

        let states = if self.additional_projections.is_empty() {
            // No additional projections, just need to wrap the function's states.
            states
                .into_iter()
                .map(|state| TableExecutePartitionState {
                    function_state: state,
                    row_batch: Batch::empty(),
                    curr_row_idx: 0,      // Doesn't matter.
                    needs_next_row: true, // Doesn't matter.
                })
                .collect()
        } else {
            // Otherwise we need to create a batch to execute one input row at a
            // time.
            states
                .into_iter()
                .map(|state| {
                    Ok(TableExecutePartitionState {
                        function_state: state,
                        row_batch: Batch::new(self.input_types.clone(), 1)?,
                        curr_row_idx: 0,
                        needs_next_row: true,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };

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
        output.reset_for_write()?;

        if self.additional_projections.is_empty() {
            // Simple case, just delegate to table function.
            return self.function.raw.call_poll_execute(
                cx,
                &operator_state.function_op_state,
                &mut state.function_state,
                input,
                output,
            );
        }

        // Otherwise we need to handle each row separately to properly expand
        // out the projected input.

        loop {
            if state.needs_next_row {
                if state.curr_row_idx >= input.num_rows() {
                    // Needs new input batch.
                    state.curr_row_idx = 0;
                    return Ok(PollExecute::NeedsMore);
                }

                // "Copy" row we're working into intermediate batch.
                state
                    .row_batch
                    .clone_row_from_other(input, state.curr_row_idx, 1)?;

                state.needs_next_row = false;
                state.curr_row_idx += 1;
            }

            // Call table func with an input batch containing only a single row.
            let poll = self.function.raw.call_poll_execute(
                cx,
                &operator_state.function_op_state,
                &mut state.function_state,
                &mut state.row_batch,
                output,
            )?;

            if poll == PollExecute::NeedsMore {
                // Need to go to next row for meaningful output.
                state.needs_next_row = true;
                continue;
            }

            // Otherwise we got output corresponding to a single row. Now add in
            // the projected inputs and extend out to the proper length.
            //
            // Additional projections are placed at the end of the batch.
            let num_rows = output.num_rows();
            let start_idx = output.arrays.len() - self.additional_projections.len();

            for (rel_idx, col_expr) in self.additional_projections.iter().enumerate() {
                let out_idx = start_idx + rel_idx;

                let input_arr = &mut state.row_batch.arrays[col_expr.idx];
                let output_arr = &mut output.arrays[out_idx];

                output_arr.clone_constant_from(input_arr, 0, num_rows, &mut NopCache)?;
            }

            return Ok(poll);
        }
    }

    fn poll_finalize_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        self.function.raw.call_poll_finalize_execute(
            cx,
            &operator_state.function_op_state,
            &mut state.function_state,
        )
    }
}

impl Explainable for PhysicalTableExecute {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent =
            ExplainEntry::new(Self::OPERATOR_NAME).with_value("function", self.function.name);
        if conf.verbose {
            ent = ent.with_values("input_types", &self.input_types);
        }

        ent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::table::TableFunctionInput;
    use crate::functions::table::builtin::series::FUNCTION_SET_GENERATE_SERIES;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::operator::OperatorWrapper;
    use crate::{expr, generate_batch};

    fn plan_generate_series() -> PlannedTableFunction {
        let input = TableFunctionInput::all_unnamed([
            expr::column((0, 0), DataType::Int64),
            expr::column((0, 1), DataType::Int64),
            expr::column((0, 2), DataType::Int64),
        ]);
        expr::bind_table_execute_function(&FUNCTION_SET_GENERATE_SERIES, input).unwrap()
    }

    #[test]
    fn execute_no_additional_projections() {
        let planned = plan_generate_series();

        let wrapper = OperatorWrapper::new(PhysicalTableExecute::new::<PhysicalColumnExpr>(
            planned,
            [],
            [DataType::Int64, DataType::Int64, DataType::Int64],
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        let mut output = Batch::new([DataType::Int64], 16).unwrap();

        // generate_series(4, 8, 2)
        // generate_series(5, 6, 1)
        let mut input = generate_batch!([4_i64, 5], [8_i64, 6], [2_i64, 1]);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([4_i64, 6, 8]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([5_i64, 6]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
    }

    #[test]
    fn execute_with_input_projection() {
        // Project the 'start' input column.

        let planned = plan_generate_series();

        let wrapper = OperatorWrapper::new(PhysicalTableExecute::new(
            planned,
            [(0, DataType::Int64)],
            [DataType::Int64, DataType::Int64, DataType::Int64],
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        let mut output = Batch::new([DataType::Int64, DataType::Int64], 16).unwrap();

        // generate_series(4, 8, 2)
        // generate_series(5, 6, 1)
        let mut input = generate_batch!([4_i64, 5], [8_i64, 6], [2_i64, 1]);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        // Note the extra column (the start value)
        let expected = generate_batch!([4_i64, 6, 8], [4_i64, 4, 4]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([5_i64, 6], [5_i64, 5]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
    }

    #[test]
    fn execute_with_additional_column() {
        // Project an additional column that's not an input to the function.

        let planned = plan_generate_series();

        let wrapper = OperatorWrapper::new(PhysicalTableExecute::new(
            planned,
            [(3, DataType::Utf8)],
            [
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Utf8,
            ],
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        let mut output = Batch::new([DataType::Int64, DataType::Utf8], 16).unwrap();

        // generate_series(4, 8, 2), 'hello'
        // generate_series(5, 6, 1), 'world'
        let mut input = generate_batch!([4_i64, 5], [8_i64, 6], [2_i64, 1], ["hello", "world"]);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([4_i64, 6, 8], ["hello", "hello", "hello"]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([5_i64, 6], ["world", "world"]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);
    }
}
