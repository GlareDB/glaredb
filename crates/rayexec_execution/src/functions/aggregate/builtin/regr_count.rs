use std::fmt::Debug;

use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::physical_type::PhysicalAnyOld;
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
pub struct RegrCount;

impl FunctionInfo for RegrCount {
    fn name(&self) -> &'static str {
        "regr_count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the count where both inputs are not NULL.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for RegrCount {
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
                function_impl: Box::new(RegrCountImpl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrCountImpl;

impl AggregateFunctionImpl for RegrCountImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_binary_aggregate_states::<PhysicalAnyOld, PhysicalAnyOld, _, _, _, _>(
            RegrCountState::default,
            move |states| primitive_finalize(DataType::Int64, states),
        )
    }
}

/// State for `regr_count`.
///
/// Note that this can be used for any input type, but the sql function we
/// expose only accepts f64 (to match Postgres).
#[derive(Debug, Clone, Copy, Default)]
pub struct RegrCountState {
    count: i64,
}

impl AggregateState<((), ()), i64> for RegrCountState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: ((), ())) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(i64, bool)> {
        Ok((self.count, true))
    }
}
