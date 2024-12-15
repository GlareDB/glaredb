use std::collections::HashMap;
use std::task::{Context, Waker};

use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::physical_type::PhysicalI64;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::field::{Field, Schema};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::{RayexecError, Result};

use crate::execution::operators::{PollFinalize, PollPull, PollPush};
use crate::expr::{self, Expression};
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
            },
            Signature {
                positional_args: &[DataTypeId::Int64, DataTypeId::Int64, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Any,
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
            positional_inputs.push(expr::lit(1 as i64))
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
                    row_idx: 0,
                    finished: false,
                    params: SeriesParams::default(),
                    push_waker: None,
                    pull_waker: None,
                }) as _
            })
            .collect();

        Ok(states)
    }
}

#[derive(Debug, Clone, Default)]
struct SeriesParams {
    batch_size: usize,
    exhausted: bool,

    curr: i64,
    stop: i64,
    step: i64,
}

impl SeriesParams {
    /// Generate the next set of rows using the current parameters.
    fn generate_next(&mut self) -> Array {
        debug_assert!(!self.exhausted);

        let mut series: Vec<i64> = Vec::new();
        if self.curr < self.stop && self.step > 0 {
            // Going up.
            let mut count = 0;
            while self.curr <= self.stop && count < self.batch_size {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        } else if self.curr > self.stop && self.step < 0 {
            // Going down.
            let mut count = 0;
            while self.curr >= self.stop && count < self.batch_size {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        }

        if series.len() < self.batch_size {
            self.exhausted = true;
        }

        // Calculate the start value for the next iteration.
        if let Some(last) = series.last() {
            self.curr = *last + self.step;
        }

        Array::new_with_array_data(DataType::Int64, PrimitiveStorage::from(series))
    }
}

#[derive(Debug)]
pub struct GenerateSeriesInOutPartitionState {
    batch_size: usize,
    /// Batch we're working on.
    batch: Option<Batch>,
    /// Row index we're on.
    row_idx: usize,
    /// If we're finished.
    finished: bool,
    /// Current params.
    params: SeriesParams,
    push_waker: Option<Waker>,
    pull_waker: Option<Waker>,
}

impl TableInOutPartitionState for GenerateSeriesInOutPartitionState {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush> {
        if self.batch.is_some() {
            // Still processing current batch, come back later.
            self.push_waker = Some(cx.waker().clone());
            if let Some(pull_waker) = self.pull_waker.take() {
                pull_waker.wake();
            }
            return Ok(PollPush::Pending(batch));
        }

        self.batch = Some(batch);
        self.row_idx = 0;

        Ok(PollPush::Pushed)
    }

    fn poll_finalize_push(&mut self, _cx: &mut Context) -> Result<PollFinalize> {
        self.finished = true;
        Ok(PollFinalize::Finalized)
    }

    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        let batch = match &self.batch {
            Some(batch) => batch,
            None => {
                if self.finished {
                    return Ok(PollPull::Exhausted);
                }

                // No batch to work on, come back later.
                self.pull_waker = Some(cx.waker().clone());
                if let Some(push_waker) = self.push_waker.take() {
                    push_waker.wake()
                }
                return Ok(PollPull::Pending);
            }
        };

        if self.params.exhausted {
            // Move to next row to process.
            self.row_idx += 1;

            if self.row_idx >= batch.num_rows() {
                // Need more input.
                self.batch = None;
                self.pull_waker = Some(cx.waker().clone());
                if let Some(push_waker) = self.push_waker.take() {
                    push_waker.wake()
                }

                return Ok(PollPull::Pending);
            }

            // Generate new params from row.
            let start =
                UnaryExecutor::value_at::<PhysicalI64>(batch.column(0).unwrap(), self.row_idx)?;
            let end =
                UnaryExecutor::value_at::<PhysicalI64>(batch.column(1).unwrap(), self.row_idx)?;
            let step =
                UnaryExecutor::value_at::<PhysicalI64>(batch.column(2).unwrap(), self.row_idx)?;

            // Use values from start/end if they're both not null. Otherwise use
            // parameters that produce an empty array.
            match (start, end, step) {
                (Some(start), Some(end), Some(step)) => {
                    self.params = SeriesParams {
                        batch_size: self.batch_size,
                        exhausted: false,
                        curr: start,
                        stop: end,
                        step,
                    }
                }
                _ => {
                    self.params = SeriesParams {
                        batch_size: self.batch_size,
                        exhausted: false,
                        curr: 1,
                        stop: 0,
                        step: 1,
                    }
                }
            }

            // TODO: Validate params.
        }

        let out = self.params.generate_next();
        let batch = Batch::try_new([out])?;

        Ok(PollPull::Computed(batch.into()))
    }
}
