use std::fmt::Debug;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, PhysicalBool};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;

pub const FUNCTION_SET_BOOL_OR: AggregateFunctionSet = AggregateFunctionSet {
    name: "bool_or",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Returns true if any non-NULL input is true, otherwise false.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &SimpleUnaryAggregate::new(&BoolOr),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct BoolOr;

impl UnaryAggregate for BoolOr {
    type Input = PhysicalBool;
    type Output = PhysicalBool;

    type BindState = ();
    type GroupState = BoolOrState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

#[derive(Debug, Default)]
pub struct BoolOrState {
    result: bool,
    valid: bool,
}

impl AggregateState<&bool, bool> for BoolOrState {
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.result = self.result || other.result;
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, _state: &(), input: &bool) -> Result<()> {
        self.result = self.result || *input;
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = bool>,
    {
        if self.valid {
            output.put(&self.result);
        } else {
            output.put_null();
        }
        Ok(())
    }
}
