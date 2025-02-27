use std::task::Context;

use rayexec_error::{OptionExt, RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::cache::NopCache;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::functions::table::{inout, PlannedTableFunction2, TableFunctionImpl2};

#[derive(Debug)]
pub struct TableInOutPartitionState {
    /// State for the table function.
    function_state: Box<dyn inout::TableInOutPartitionState>,
    /// Batch for holding a single row from the input when we're projecting out
    /// the input.
    ///
    /// Only used if projecting input.
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

#[derive(Debug)]
pub struct PhysicalTableInOut {
    /// The table function.
    pub function: PlannedTableFunction2,
    /// Data types for the input into this operator.
    pub input_types: Vec<DataType>,
    /// Column expressions for columns that should be projected out of this
    /// operator alongside the results of the table function.
    ///
    /// If empty, then the only output for this operator will be the result of
    /// the table function.
    ///
    /// Projected inputs are ordered after the outputs of the table in/out
    /// output.
    pub projected_inputs: Vec<PhysicalColumnExpr>,
}

impl ExecutableOperator for PhysicalTableInOut {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        _batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let states = match &self.function.function_impl {
            TableFunctionImpl2::InOut(inout) => inout.create_states(partitions)?,
            _ => {
                return Err(RayexecError::new(format!(
                    "'{}' is not a table in/out function",
                    self.function.function.name()
                )))
            }
        };

        let states = states
            .into_iter()
            .map(|state| {
                Ok(PartitionState::TableInOut(TableInOutPartitionState {
                    function_state: state,
                    row_batch: Batch::new(self.input_types.clone(), 1)?,
                    curr_row_idx: 0,
                    needs_next_row: true,
                }))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(UnaryInputStates {
            operator_state: OperatorState::None,
            partition_states: states,
        })
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::TableInOut(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if self.projected_inputs.is_empty() {
            // Simple case, just delegate to table function.
            return state.function_state.poll_execute(cx, inout);
        }

        // Otherwise we need to handle each row separately to properly expand
        // out the projected input.
        let output = inout.output.required("output batch required")?;
        let input = inout.input.required("input batch required")?;

        loop {
            if state.needs_next_row {
                if state.curr_row_idx >= input.num_rows() {
                    // Needs new input batch.
                    state.curr_row_idx = 0;
                    return Ok(PollExecute::NeedsMore);
                }

                // "Copy" row we're working into intermediate batch.
                state.row_batch.clone_from_other(input)?;

                state
                    .row_batch
                    .select(Selection::slice(&[state.curr_row_idx]))?;
                state.needs_next_row = false;
                state.curr_row_idx += 1;
            }

            // Call table func with an input batch containing only a single row.
            let poll = state.function_state.poll_execute(
                cx,
                ExecuteInOutState {
                    input: Some(&mut state.row_batch),
                    output: Some(output),
                },
            )?;

            if poll == PollExecute::NeedsMore {
                // Need to go to next row for meaningful output.
                state.needs_next_row = true;
                continue;
            }

            // Otherwise we got output corresponding to a single row. Now add in
            // the projected inputs and extend out to the proper length.
            //
            // Inputs are located at the end of the output batch.
            let num_rows = output.num_rows();
            let start_idx = output.arrays.len() - self.projected_inputs.len();

            for (rel_idx, col_expr) in self.projected_inputs.iter().enumerate() {
                let out_idx = start_idx + rel_idx;

                let input_arr = &mut state.row_batch.arrays[col_expr.idx];
                let output_arr = &mut output.arrays[out_idx];

                output_arr.clone_constant_from(input_arr, 0, num_rows, &mut NopCache)?;
            }

            return Ok(poll);
        }
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::TableInOut(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        state.function_state.poll_finalize(cx)
    }
}

impl Explainable for PhysicalTableInOut {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableInOut")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::expr::column_expr::{ColumnExpr, ColumnReference};
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::Expression;
    use crate::functions::table::builtin::series::GenerateSeriesInOutPlanner;
    use crate::functions::table::InOutPlanner2;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::database_context::test_database_context;
    use crate::testutil::operator::OperatorWrapper;

    fn plan_generate_series() -> PlannedTableFunction2 {
        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int64, DataType::Int64, DataType::Int64],
                vec!["start".to_string(), "stop".to_string(), "step".to_string()],
            )
            .unwrap();

        GenerateSeriesInOutPlanner
            .plan(
                &table_list,
                vec![
                    Expression::Column(ColumnExpr {
                        reference: ColumnReference {
                            table_scope: table_ref,
                            column: 0,
                        },
                        datatype: DataType::Int64,
                    }),
                    Expression::Column(ColumnExpr {
                        reference: ColumnReference {
                            table_scope: table_ref,
                            column: 1,
                        },
                        datatype: DataType::Int64,
                    }),
                    Expression::Column(ColumnExpr {
                        reference: ColumnReference {
                            table_scope: table_ref,
                            column: 2,
                        },
                        datatype: DataType::Int64,
                    }),
                ],
                HashMap::new(),
            )
            .unwrap()
    }

    #[test]
    fn inout_no_input_project() {
        let mut wrapper = OperatorWrapper::new(PhysicalTableInOut {
            function: plan_generate_series(),
            input_types: vec![DataType::Int64; 3],
            projected_inputs: Vec::new(),
        });

        let mut states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();

        let mut output = Batch::new([DataType::Int64], 1024).unwrap();
        // generate_series(4, 8, 2)
        // generate_series(5, 6, 1)
        let mut input = Batch::from_arrays([
            Array::try_from_iter([4_i64, 5]).unwrap(),
            Array::try_from_iter([8_i64, 6]).unwrap(),
            Array::try_from_iter([2_i64, 1]).unwrap(),
        ])
        .unwrap();

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = Batch::from_arrays([Array::try_from_iter([4_i64, 6, 8]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        // Keep polling for the rest...
    }

    #[test]
    fn inout_with_input_project() {
        let mut wrapper = OperatorWrapper::new(PhysicalTableInOut {
            function: plan_generate_series(),
            input_types: vec![DataType::Int64; 3],
            projected_inputs: vec![
                PhysicalColumnExpr {
                    idx: 1,
                    datatype: DataType::Int64,
                },
                PhysicalColumnExpr {
                    idx: 0,
                    datatype: DataType::Int64,
                },
            ],
        });

        let mut states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();

        let mut output =
            Batch::new([DataType::Int64, DataType::Int64, DataType::Int64], 1024).unwrap();
        // generate_series(4, 8, 2)
        // generate_series(5, 6, 1)
        let mut input = Batch::from_arrays([
            Array::try_from_iter([4_i64, 5]).unwrap(),
            Array::try_from_iter([8_i64, 6]).unwrap(),
            Array::try_from_iter([2_i64, 1]).unwrap(),
        ])
        .unwrap();

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = Batch::from_arrays([
            Array::try_from_iter([4_i64, 6, 8]).unwrap(),
            Array::try_from_iter([8_i64, 8, 8]).unwrap(), // 'stop'
            Array::try_from_iter([4_i64, 4, 4]).unwrap(), // 'start'
        ])
        .unwrap();
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = Batch::from_arrays([
            Array::try_from_iter([5_i64, 6]).unwrap(),
            Array::try_from_iter([6_i64, 6]).unwrap(), // 'stop'
            Array::try_from_iter([5_i64, 5]).unwrap(), // 'start'
        ])
        .unwrap();

        assert_batches_eq(&expected, &output);

        // Keep polling for the rest...
    }
}
