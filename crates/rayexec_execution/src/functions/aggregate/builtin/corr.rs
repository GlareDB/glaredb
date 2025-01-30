use std::fmt::Debug;

use rayexec_error::Result;

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{StddevPopFinalize, VarianceState};
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    binary_update,
    drain,
    AggregateGroupStates,
    TypedAggregateGroupStates,
};
use crate::functions::aggregate::{
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

/// Pearson coefficient, population.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Corr;

impl FunctionInfo for Corr {
    fn name(&self) -> &'static str {
        "corr"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Return the population correlation coefficient.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for Corr {
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
                function_impl: Box::new(CorrImpl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrImpl;

impl AggregateFunctionImpl for CorrImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            CorrelationState::default,
            binary_update::<PhysicalF64, PhysicalF64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
    }
}

#[derive(Debug, Default)]
pub struct CorrelationState {
    covar: CovarState<CovarPopFinalize>,
    stddev_x: VarianceState<StddevPopFinalize>,
    stddev_y: VarianceState<StddevPopFinalize>,
}

impl CorrelationState {
    pub fn finalize_value(&self) -> Option<f64> {
        let cov = self.covar.finalize_value()?;
        let stddev_x = self.stddev_x.finalize_value()?;
        let stddev_y = self.stddev_y.finalize_value()?;

        let div = stddev_x * stddev_y;
        if div == 0.0 {
            // Return null, matches Postgres.
            //
            // Note duckdb returns NaN here.
            return None;
        }

        Some(cov / div)
    }
}

impl AggregateState<(&f64, &f64), f64> for CorrelationState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.covar.merge(&mut other.covar)?;
        self.stddev_x.merge(&mut other.stddev_x)?;
        self.stddev_y.merge(&mut other.stddev_y)?;
        Ok(())
    }

    fn update(&mut self, input: (&f64, &f64)) -> Result<()> {
        self.covar.update(input)?;

        // Note input is passed in as (y, x)
        self.stddev_x.update(input.1)?;
        self.stddev_y.update(input.0)?;

        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = f64>,
        B: BufferManager,
    {
        match self.finalize_value() {
            Some(val) => output.put(&val),
            None => output.put_null(),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_state_single_input() {
        let mut state = CorrelationState::default();
        state.update((&1.0, &1.0)).unwrap();

        let v = state.finalize_value();
        assert_eq!(None, v);
    }
}
