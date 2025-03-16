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
use crate::functions::{FunctionInfo, Signature};

// Min/max is used in some aggregate layout tests, assuming the size and
// alignment of min/max primitive states.
//
// If we change the state from just being a val+bool, those tests may need
// updating.

pub const FUNCTION_SET_MIN: AggregateFunctionSet = AggregateFunctionSet {
    name: "min",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the minimum non-NULL value seen from input.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalBool>::new()),
        ),
        // Ints
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int16),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int32),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int128], DataTypeId::Int128),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI128>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt8], DataTypeId::UInt8),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalU8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt16], DataTypeId::UInt16),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalU16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt32], DataTypeId::UInt32),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalU32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt64], DataTypeId::UInt64),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalU64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt128], DataTypeId::UInt128),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalU128>::new()),
        ),
        // Floats
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalF16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalF32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalF64>::new()),
        ),
        // Decimal
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal64], DataTypeId::Decimal64),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal128], DataTypeId::Decimal128),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI128>::new()),
        ),
        // Date/time
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Date32], DataTypeId::Date32),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Date64], DataTypeId::Date64),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Timestamp], DataTypeId::Timestamp),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Interval], DataTypeId::Interval),
            &SimpleUnaryAggregate::new(&MinPrimitive::<PhysicalInterval>::new()),
        ),
        // String/binary
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
            &SimpleUnaryAggregate::new(&MinBinary),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Binary], DataTypeId::Binary),
            &SimpleUnaryAggregate::new(&MinBinary),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinPrimitive<S> {
    _s: PhantomData<S>,
}

impl<S> MinPrimitive<S> {
    pub const fn new() -> Self {
        MinPrimitive { _s: PhantomData }
    }
}

impl<S> UnaryAggregate for MinPrimitive<S>
where
    S: MutableScalarStorage,
    S::StorageType: PartialOrd + Default + Copy + Sized,
{
    type Input = S;
    type Output = S;

    type BindState = ();
    type GroupState = MinStatePrimitive<S::StorageType>;

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
pub struct MinBinary;

impl UnaryAggregate for MinBinary {
    type Input = PhysicalBinary;
    type Output = PhysicalBinary;

    type BindState = ();
    type GroupState = MinStateBinary;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: inputs[0].datatype()?,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        MinStateBinary::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Min;

impl FunctionInfo for Min {
    fn name(&self) -> &'static str {
        "min"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Any],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Return the minimum non-NULL value seen from input.",
                arguments: &["input"],
                example: None,
            }),
        }]
    }
}

pub const FUNCTION_SET_MAX: AggregateFunctionSet = AggregateFunctionSet {
    name: "max",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the maximum non-NULL value seen from input.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalBool>::new()),
        ),
        // Ints
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int16),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int32),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int128], DataTypeId::Int128),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI128>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt8], DataTypeId::UInt8),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalU8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt16], DataTypeId::UInt16),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalU16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt32], DataTypeId::UInt32),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalU32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt64], DataTypeId::UInt64),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalU64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::UInt128], DataTypeId::UInt128),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalU128>::new()),
        ),
        // Floats
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalF16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalF32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalF64>::new()),
        ),
        // Decimal
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal64], DataTypeId::Decimal64),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal128], DataTypeId::Decimal128),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI128>::new()),
        ),
        // Date/time
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Date32], DataTypeId::Date32),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Date64], DataTypeId::Date64),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Timestamp], DataTypeId::Timestamp),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Interval], DataTypeId::Interval),
            &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalInterval>::new()),
        ),
        // String/binary
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
            &SimpleUnaryAggregate::new(&MaxBinary),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Binary], DataTypeId::Binary),
            &SimpleUnaryAggregate::new(&MaxBinary),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxPrimitive<S> {
    _s: PhantomData<S>,
}

impl<S> MaxPrimitive<S> {
    pub const fn new() -> Self {
        MaxPrimitive { _s: PhantomData }
    }
}

impl<S> UnaryAggregate for MaxPrimitive<S>
where
    S: MutableScalarStorage,
    S::StorageType: PartialOrd + Default + Copy + Sized,
{
    type Input = S;
    type Output = S;

    type BindState = ();
    type GroupState = MaxStatePrimitive<S::StorageType>;

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
pub struct MaxBinary;

impl UnaryAggregate for MaxBinary {
    type Input = PhysicalBinary;
    type Output = PhysicalBinary;

    type BindState = ();
    type GroupState = MaxStateBinary;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: inputs[0].datatype()?,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        MaxStateBinary::default()
    }
}

#[derive(Debug, Default)]
pub struct MaxStatePrimitive<T> {
    max: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for MaxStatePrimitive<T>
where
    T: Debug + Sync + Send + PartialOrd + Copy,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.max, &mut other.max);
            return Ok(());
        }

        if !other.valid {
            return Ok(());
        }

        if self.max.lt(&other.max) {
            std::mem::swap(&mut self.max, &mut other.max);
        }

        Ok(())
    }

    fn update(&mut self, _state: &(), input: &T) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.max = *input;
            return Ok(());
        }

        if self.max.lt(input) {
            self.max = *input;
        }

        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.max);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MaxStateBinary {
    max: Vec<u8>,
    valid: bool,
}

impl AggregateState<&[u8], [u8]> for MaxStateBinary {
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.max, &mut other.max);
            return Ok(());
        }

        if !other.valid {
            return Ok(());
        }

        if self.max.lt(&other.max) {
            std::mem::swap(&mut self.max, &mut other.max);
        }

        Ok(())
    }

    fn update(&mut self, _state: &(), input: &[u8]) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.max = input.to_vec();
            return Ok(());
        }

        if self.max.as_slice().lt(input) {
            self.max = input.to_vec();
        }

        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = [u8]>,
    {
        if self.valid {
            output.put(&self.max);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MinStatePrimitive<T> {
    min: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for MinStatePrimitive<T>
where
    T: Debug + Sync + Send + PartialOrd + Copy,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.min, &mut other.min);
            return Ok(());
        }

        if !other.valid {
            return Ok(());
        }

        if self.min.gt(&other.min) {
            std::mem::swap(&mut self.min, &mut other.min);
        }

        Ok(())
    }

    fn update(&mut self, _state: &(), input: &T) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.min = *input;
            return Ok(());
        }

        if self.min.gt(input) {
            self.min = *input;
        }

        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.min);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MinStateBinary {
    min: Vec<u8>,
    valid: bool,
}

impl AggregateState<&[u8], [u8]> for MinStateBinary {
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.min, &mut other.min);
            return Ok(());
        }

        if !other.valid {
            return Ok(());
        }

        if self.min.gt(&other.min) {
            std::mem::swap(&mut self.min, &mut other.min);
        }

        Ok(())
    }

    fn update(&mut self, _state: &(), input: &[u8]) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.min = input.to_vec();
            return Ok(());
        }

        if self.min.as_slice().gt(input) {
            self.min = input.to_vec();
        }

        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = [u8]>,
    {
        if self.valid {
            output.put(&self.min);
        } else {
            output.put_null();
        }

        Ok(())
    }
}
