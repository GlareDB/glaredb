use std::fmt::Debug;

use rayexec_error::Result;

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{VariancePopFinalize, VarianceState};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::physical_type::PhysicalF64;
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
pub struct RegrSlope;

impl FunctionInfo for RegrSlope {
    fn name(&self) -> &'static str {
        "regr_slope"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the slope of the least-squares-fit linear equation.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for RegrSlope {
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
                function_impl: Box::new(RegrSlopeImpl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrSlopeImpl;

impl AggregateFunctionImpl for RegrSlopeImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_binary_aggregate_states::<PhysicalF64, PhysicalF64, _, _, _, _>(
            RegrSlopeState::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Default)]
pub struct RegrSlopeState {
    cov: CovarState<CovarPopFinalize>,
    var: VarianceState<VariancePopFinalize>,
}

impl AggregateState<(f64, f64), f64> for RegrSlopeState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.cov.merge(&mut other.cov)?;
        self.var.merge(&mut other.var)?;
        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        self.cov.update(input)?;
        self.var.update(input.1)?; // Update with 'x'
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        let (cov, cov_valid) = self.cov.finalize()?;
        let (var, var_valid) = self.var.finalize()?;

        if cov_valid && var_valid {
            if var == 0.0 {
                return Ok((0.0, false));
            }
            let v = cov / var;
            Ok((v, true))
        } else {
            Ok((0.0, false))
        }
    }
}
