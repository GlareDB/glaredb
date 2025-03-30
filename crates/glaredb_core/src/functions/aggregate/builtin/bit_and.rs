use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::BitAnd;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
};
use crate::arrays::datatype::DataTypeId;
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;

pub const FUNCTION_SET_BIT_AND: AggregateFunctionSet = AggregateFunctionSet {
    name: "bit_and",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Returns the bitwise AND of all non-NULL input values.",
        arguments: &["integer"],
        example: None,
    }),
    functions: &[
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalI8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int16),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalI16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int32),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt8], DataTypeId::UInt8),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalU8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt16], DataTypeId::UInt16),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalU16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt32], DataTypeId::UInt32),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalU32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt64], DataTypeId::UInt64),
            &SimpleUnaryAggregate::new(&BitAndPrimitive::<PhysicalU64>::new()),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitAndPrimitive<S> {
    _s: PhantomData<S>,
}

impl<S> BitAndPrimitive<S> {
    pub const fn new() -> Self {
        BitAndPrimitive { _s: PhantomData }
    }
}

impl<S> UnaryAggregate for BitAndPrimitive<S>
where
    S: MutableScalarStorage,
    S::StorageType: BitAnd<Output = S::StorageType> + Copy + Sized + Debug + Default,
{
    type Input = S;
    type Output = S;

    type BindState = ();
    type GroupState = BitAndStatePrimitive<S::StorageType>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: inputs[0].datatype()?,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

#[derive(Debug, Default)]
pub struct BitAndStatePrimitive<T: Default> {
    result: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for BitAndStatePrimitive<T>
where
    T: Debug + Sync + Send + BitAnd<Output = T> + Copy + Default,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.result, &mut other.result);
            return Ok(());
        }

        if !other.valid {
            return Ok(());
        }

        self.result = self.result & other.result;

        Ok(())
    }

    fn update(&mut self, _state: &(), input: &T) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.result = *input;
            return Ok(());
        }

        self.result = self.result & *input;

        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.result);
        } else {
            output.put_null();
        }

        Ok(())
    }
}
