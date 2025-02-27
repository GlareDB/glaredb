use std::fmt::Debug;

use rayexec_error::Result;

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{VariancePopFinalize, VarianceState};
use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::simple::{BinaryAggregate, SimpleBinaryAggregate};
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::functions::Signature;

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
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.cov.merge(&mut other.cov)?;
        self.var.merge(&mut other.var)?;
        Ok(())
    }

    fn update(&mut self, input: (&f64, &f64)) -> Result<()> {
        self.cov.update(input)?;
        self.var.update(input.1)?; // Update with 'x'
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
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
