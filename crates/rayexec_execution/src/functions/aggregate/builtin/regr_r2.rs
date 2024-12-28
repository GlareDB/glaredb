use std::fmt::Debug;

use rayexec_error::Result;

use super::corr::CorrelationState;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::physical_type::PhysicalF64_2;
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
pub struct RegrR2;

impl FunctionInfo for RegrR2 {
    fn name(&self) -> &'static str {
        "regr_r2"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the square of the correlation coefficient.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for RegrR2 {
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
                function_impl: Box::new(RegrR2Impl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegrR2Impl;

impl AggregateFunctionImpl for RegrR2Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_binary_aggregate_states::<PhysicalF64_2, PhysicalF64_2, _, _, _, _>(
            RegrR2State::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Default)]
pub struct RegrR2State {
    corr: CorrelationState,
}

impl AggregateState<(f64, f64), f64> for RegrR2State {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.corr.merge(&mut other.corr)?;
        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        self.corr.update(input)?;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        let (v, valid) = self.corr.finalize()?;
        if valid {
            Ok((v.powi(2), true))
        } else {
            Ok((0.0, false))
        }
    }
}
