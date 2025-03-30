use std::fmt::Debug;

use glaredb_error::Result;

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{StddevPopFinalize, VarianceState};
use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::aggregate::simple::{BinaryAggregate, SimpleBinaryAggregate};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;

pub const FUNCTION_SET_CORR: AggregateFunctionSet = AggregateFunctionSet {
    name: "corr",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the (Pearson) population correlation coefficient.",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&Corr),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Corr;

impl BinaryAggregate for Corr {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = CorrelationState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        CorrelationState::default()
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
    type BindState = ();

    fn merge(&mut self, state: &(), other: &mut Self) -> Result<()> {
        self.covar.merge(state, &mut other.covar)?;
        self.stddev_x.merge(state, &mut other.stddev_x)?;
        self.stddev_y.merge(state, &mut other.stddev_y)?;
        Ok(())
    }

    fn update(&mut self, state: &(), input: (&f64, &f64)) -> Result<()> {
        self.covar.update(state, input)?;

        // Note input is passed in as (y, x)
        self.stddev_x.update(state, input.1)?;
        self.stddev_y.update(state, input.0)?;

        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
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
        state.update(&(), (&1.0, &1.0)).unwrap();

        let v = state.finalize_value();
        assert_eq!(None, v);
    }
}
