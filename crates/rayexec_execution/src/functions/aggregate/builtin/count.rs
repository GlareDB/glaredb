use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::physical_type::PhysicalAny;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::aggregate::states::{new_unary_aggregate_states, AggregateGroupStates};
use crate::functions::aggregate::{
    primitive_finalize,
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count;

impl FunctionInfo for Count {
    fn name(&self) -> &'static str {
        "count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Int64,
        }]
    }
}

impl AggregateFunction for Count {
    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: DataType::Int64,
            inputs,
            function_impl: Box::new(CountNonNullImpl),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CountNonNullImpl;

impl AggregateFunctionImpl for CountNonNullImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalAny, _, _, _, _>(
            CountNonNullState::default,
            move |states| primitive_finalize(DataType::Int64, states),
        )
    }
}

#[derive(Debug, Default)]
pub struct CountNonNullState {
    count: i64,
}

impl AggregateState<(), i64> for CountNonNullState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: ()) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(i64, bool)> {
        // Always valid, even when count is 0
        Ok((self.count, true))
    }
}
