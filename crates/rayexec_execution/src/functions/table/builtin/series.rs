use std::collections::HashMap;
use std::task::{Context, Waker};

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array2;
use crate::arrays::batch::Batch2;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::physical_type::PhysicalI64_2;
use crate::arrays::executor::scalar::UnaryExecutor2;
use crate::arrays::field::{Field, Schema};
use crate::arrays::scalar::OwnedScalarValue;
use crate::arrays::storage::PrimitiveStorage;
use crate::execution::operators::{PollFinalize, PollPush};
use crate::expr::{self, Expression};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::table::inout::{InOutPollPull, TableInOutFunction, TableInOutPartitionState};
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
                    batch_size: 1024, // TODO
                    batch: None,
                    next_row_idx: 0,
                    finished: false,
                    params: SeriesParams {
                        exhausted: true, // Triggers param update on first pull
                        current_row_idx: 0,
                        curr: 0,
                        stop: 0,
                        step: 0,
                    },
                    push_waker: None,
                    pull_waker: None,
                }) as _
            })
            .collect();

        Ok(states)
    }
}

#[derive(Debug, Clone)]
struct SeriesParams {
    exhausted: bool,

    /// Index of the row these parameters were generated from.
    current_row_idx: usize,

    curr: i64,
    stop: i64,
    step: i64,
}

impl SeriesParams {
    /// Generate the next set of rows using the current parameters.
    fn generate_next(&mut self, batch_size: usize) -> Array2 {
        debug_assert!(!self.exhausted);

        let mut series: Vec<i64> = Vec::new();
        if self.curr < self.stop && self.step > 0 {
            // Going up.
            let mut count = 0;
            while self.curr <= self.stop && count < batch_size {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        } else if self.curr > self.stop && self.step < 0 {
            // Going down.
            let mut count = 0;
            while self.curr >= self.stop && count < batch_size {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        }

        if series.len() < batch_size {
            self.exhausted = true;
        }

        // Calculate the start value for the next iteration.
        if let Some(last) = series.last() {
            self.curr = *last + self.step;
        }

        Array2::new_with_array_data(DataType::Int64, PrimitiveStorage::from(series))
    }
}

#[derive(Debug)]
pub struct GenerateSeriesInOutPartitionState {
    batch_size: usize,
    /// Batch we're working on.
    batch: Option<Batch2>,
    /// Current row number
    next_row_idx: usize,
    /// If we're finished.
    finished: bool,
    /// Current params.
    params: SeriesParams,
    push_waker: Option<Waker>,
    pull_waker: Option<Waker>,
}

impl TableInOutPartitionState for GenerateSeriesInOutPartitionState {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch2) -> Result<PollPush> {
        if self.batch.is_some() {
            // Still processing current batch, come back later.
            self.push_waker = Some(cx.waker().clone());
            if let Some(pull_waker) = self.pull_waker.take() {
                pull_waker.wake();
            }
            return Ok(PollPush::Pending(batch));
        }

        self.batch = Some(batch);
        self.next_row_idx = 0;

        Ok(PollPush::Pushed)
    }

    fn poll_finalize_push(&mut self, _cx: &mut Context) -> Result<PollFinalize> {
        self.finished = true;
        if let Some(waker) = self.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalize::Finalized)
    }

    fn poll_pull(&mut self, cx: &mut Context) -> Result<InOutPollPull> {
        if self.params.exhausted {
            let batch = match &self.batch {
                Some(batch) => batch,
                None => {
                    if self.finished {
                        return Ok(InOutPollPull::Exhausted);
                    }

                    // No batch to work on, come back later.
                    self.pull_waker = Some(cx.waker().clone());
                    if let Some(push_waker) = self.push_waker.take() {
                        push_waker.wake()
                    }
                    return Ok(InOutPollPull::Pending);
                }
            };

            // Generate new params from row.
            let start = UnaryExecutor2::value_at::<PhysicalI64_2>(
                batch.column(0).unwrap(),
                self.next_row_idx,
            )?;
            let end = UnaryExecutor2::value_at::<PhysicalI64_2>(
                batch.column(1).unwrap(),
                self.next_row_idx,
            )?;
            let step = UnaryExecutor2::value_at::<PhysicalI64_2>(
                batch.column(2).unwrap(),
                self.next_row_idx,
            )?;

            // Use values from start/end if they're both not null. Otherwise use
            // parameters that produce an empty array.
            match (start, end, step) {
                (Some(start), Some(end), Some(step)) => {
                    if step == 0 {
                        return Err(RayexecError::new("'step' may not be zero"));
                    }

                    self.params = SeriesParams {
                        exhausted: false,
                        current_row_idx: self.next_row_idx,
                        curr: start,
                        stop: end,
                        step,
                    }
                }
                _ => {
                    self.params = SeriesParams {
                        exhausted: false,
                        current_row_idx: self.next_row_idx,
                        curr: 1,
                        stop: 0,
                        step: 1,
                    }
                }
            }

            // Increment next row to use when current row exhausted.
            self.next_row_idx += 1;
            if self.next_row_idx >= batch.num_rows() {
                // Need more input.
                self.batch = None;
            }
        }

        let out = self.params.generate_next(self.batch_size);
        let batch = Batch2::try_new([out])?;

        let row_nums = vec![self.params.current_row_idx; batch.num_rows()];

        Ok(InOutPollPull::Batch { batch, row_nums })
    }
}
