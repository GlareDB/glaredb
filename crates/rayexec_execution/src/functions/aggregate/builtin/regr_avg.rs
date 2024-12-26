use std::fmt::Debug;
use std::marker::PhantomData;

use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::physical_type::PhysicalF64;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::aggregate::states::{
    new_binary_aggregate_states,
    primitive_finalize,
    AggregateGroupStates,
};
use crate::functions::aggregate::{
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrAvgY;

impl FunctionInfo for RegrAvgY {
    fn name(&self) -> &'static str {
        "regr_avgy"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the average of the dependent variable ('y').",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for RegrAvgY {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float64, DataType::Float64) => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(RegrAvgYImpl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegrAvgYImpl;

impl AggregateFunctionImpl for RegrAvgYImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_binary_aggregate_states::<PhysicalF64, PhysicalF64, _, _, _, _>(
            RegrAvgState::<Self>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

impl RegrAvgInput for RegrAvgYImpl {
    fn input(vals: (f64, f64)) -> f64 {
        // 'y' in (y, x)
        vals.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrAvgX;

impl FunctionInfo for RegrAvgX {
    fn name(&self) -> &'static str {
        "regr_avgx"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the average of the independent variable ('x').",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for RegrAvgX {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float64, DataType::Float64) => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(RegrAvgXImpl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegrAvgXImpl;

impl AggregateFunctionImpl for RegrAvgXImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_binary_aggregate_states::<PhysicalF64, PhysicalF64, _, _, _, _>(
            RegrAvgState::<Self>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

impl RegrAvgInput for RegrAvgXImpl {
    fn input(vals: (f64, f64)) -> f64 {
        // 'x' in (y, x)
        vals.1
    }
}

pub trait RegrAvgInput: Sync + Send + Default + Debug + 'static {
    fn input(vals: (f64, f64)) -> f64;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RegrAvgState<F>
where
    F: RegrAvgInput,
{
    sum: f64,
    count: i64,
    _input: PhantomData<F>,
}

impl<F> AggregateState<(f64, f64), f64> for RegrAvgState<F>
where
    F: RegrAvgInput,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        self.sum += other.sum;
        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        self.sum += F::input(input);
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        if self.count == 0 {
            Ok((0.0, false))
        } else {
            Ok((self.sum / self.count as f64, true))
        }
    }
}
