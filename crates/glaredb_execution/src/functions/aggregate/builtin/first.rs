use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use crate::arrays::datatype::DataTypeId;
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::functions::Signature;

pub const FUNCTION_SET_FIRST: AggregateFunctionSet = AggregateFunctionSet {
    name: "first",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the first non-NULL value.",
        arguments: &["input"],
        example: None,
    }),
    // TODO: Do I care that this is long?
    functions: &[
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalBool>::new()),
        ),
        // Ints
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int16),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int32),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int128], DataTypeId::Int128),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI128>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt8], DataTypeId::UInt8),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalU8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt16], DataTypeId::UInt16),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalU16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt32], DataTypeId::UInt32),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalU32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt64], DataTypeId::UInt64),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalU64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt128], DataTypeId::UInt128),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalU128>::new()),
        ),
        // Floats
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalF16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalF32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalF64>::new()),
        ),
        // Decimal
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal64], DataTypeId::Decimal64),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal128], DataTypeId::Decimal128),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI128>::new()),
        ),
        // Date/time
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Date32], DataTypeId::Date32),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Date64], DataTypeId::Date64),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Timestamp], DataTypeId::Timestamp),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Interval], DataTypeId::Interval),
            &SimpleUnaryAggregate::new(&FirstPrimitive::<PhysicalInterval>::new()),
        ),
        // String/binary
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
            &SimpleUnaryAggregate::new(&FirstBinary),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Binary], DataTypeId::Binary),
            &SimpleUnaryAggregate::new(&FirstBinary),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirstPrimitive<S> {
    _s: PhantomData<S>,
}

impl<S> FirstPrimitive<S> {
    pub const fn new() -> Self {
        FirstPrimitive { _s: PhantomData }
    }
}

impl<S> UnaryAggregate for FirstPrimitive<S>
where
    S: MutableScalarStorage,
    S::StorageType: Default + Copy,
{
    type Input = S;
    type Output = S;

    type BindState = ();
    type GroupState = FirstPrimitiveState<S::StorageType>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirstBinary;

impl UnaryAggregate for FirstBinary {
    type Input = PhysicalBinary;
    type Output = PhysicalBinary;

    type BindState = ();
    type GroupState = FirstBinaryState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: inputs[0].datatype()?,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        FirstBinaryState::default()
    }
}

#[derive(Debug, Default)]
pub struct FirstPrimitiveState<T> {
    value: Option<T>,
}

impl<T> AggregateState<&T, T> for FirstPrimitiveState<T>
where
    T: Debug + Default + Copy + Sync + Send,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        if self.value.is_none() {
            std::mem::swap(&mut self.value, &mut other.value);
        }
        Ok(())
    }

    fn update(&mut self, _state: &(), &input: &T) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input);
        }
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        match &self.value {
            Some(val) => output.put(val),
            None => output.put_null(),
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct FirstBinaryState {
    value: Option<Vec<u8>>,
}

impl AggregateState<&[u8], [u8]> for FirstBinaryState {
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        if self.value.is_none() {
            std::mem::swap(&mut self.value, &mut other.value);
        }
        Ok(())
    }

    fn update(&mut self, _state: &(), input: &[u8]) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input.to_vec());
        }
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = [u8]>,
    {
        match &self.value {
            Some(val) => output.put(val),
            None => output.put_null(),
        }
        Ok(())
    }
}
