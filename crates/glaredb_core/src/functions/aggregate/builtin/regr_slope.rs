use std::fmt::Debug;

use glaredb_error::Result;

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{VariancePopFinalize, VarianceState};
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

pub const FUNCTION_SET_REGR_SLOPE: AggregateFunctionSet = AggregateFunctionSet {
    name: "regr_slope",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the slope of the least-squares-fit linear equation.",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&RegrSlope),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrSlope;

impl BinaryAggregate for RegrSlope {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = RegrSlopeState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        RegrSlopeState::default()
    }
}

#[derive(Debug, Default)]
pub struct RegrSlopeState {
    cov: CovarState<CovarPopFinalize>,
    var: VarianceState<VariancePopFinalize>,
}

impl AggregateState<(&f64, &f64), f64> for RegrSlopeState {
    type BindState = ();

    fn merge(&mut self, state: &(), other: &mut Self) -> Result<()> {
        self.cov.merge(state, &mut other.cov)?;
        self.var.merge(state, &mut other.var)?;
        Ok(())
    }

    fn update(&mut self, state: &(), input: (&f64, &f64)) -> Result<()> {
        self.cov.update(state, input)?;
        self.var.update(state, input.1)?; // Update with 'x'
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        match (self.cov.finalize_value(), self.var.finalize_value()) {
            (Some(cov), Some(var)) => {
                if var == 0.0 {
                    output.put_null();
                    return Ok(());
                }
                let v = cov / var;
                output.put(&v);
            }
            _ => output.put_null(),
        }
        Ok(())
    }
}
