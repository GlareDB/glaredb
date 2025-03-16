use std::fmt::Debug;

use glaredb_error::Result;

use super::corr::CorrelationState;
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

pub const FUNCTION_SET_REGR_R2: AggregateFunctionSet = AggregateFunctionSet {
    name: "regr_r2",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the square of the correlation coefficient.",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&RegrR2),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrR2;

impl BinaryAggregate for RegrR2 {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = RegrR2State;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        RegrR2State::default()
    }
}

#[derive(Debug, Default)]
pub struct RegrR2State {
    corr: CorrelationState,
}

impl AggregateState<(&f64, &f64), f64> for RegrR2State {
    type BindState = ();

    fn merge(&mut self, state: &(), other: &mut Self) -> Result<()> {
        self.corr.merge(state, &mut other.corr)?;
        Ok(())
    }

    fn update(&mut self, state: &(), input: (&f64, &f64)) -> Result<()> {
        self.corr.update(state, input)?;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        match self.corr.finalize_value() {
            Some(val) => {
                let val = val.powi(2);
                output.put(&val);
            }
            None => output.put_null(),
        }
        Ok(())
    }
}
