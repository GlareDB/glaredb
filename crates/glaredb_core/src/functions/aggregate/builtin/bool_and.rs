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

pub const FUNCTION_SET_BOOL_AND: AggregateFunctionSet = AggregateFunctionSet {
    name: "bool_and",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Returns true if all non-NULL inputs are true, otherwise false.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &SimpleUnaryAggregate::new(&BoolAnd),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct BoolAnd;

impl UnaryAggregate for BoolAnd {
    type Input = PhysicalBool;
    type Output = PhysicalBool;

    type BindState = ();
    type GroupState = BoolAndState;

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

#[derive(Debug)]
pub struct BoolAndState {
    result: bool,
    valid: bool,
}

impl Default for BoolAndState {
    fn default() -> Self {
        Self {
            result: true, // Initialize to true so that AND operations work correctly
            valid: false,
        }
    }
}

impl AggregateState<&bool, bool> for BoolAndState {
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.result = self.result && other.result;
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, _state: &(), input: &bool) -> Result<()> {
        self.result = self.result && *input;
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
