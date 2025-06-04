use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;

use glaredb_error::Result;
use num_traits::{AsPrimitive, CheckedAdd};

use crate::arrays::array::physical_type::{
    AddressableMut,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    ScalarStorage,
};
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::{AggregateFunctionSet, FnName};

pub const FUNCTION_SET_SUM: AggregateFunctionSet = AggregateFunctionSet {
    name: FnName::default("sum"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::GENERAL_PURPOSE_AGGREGATE,
        description: "Compute the sum of all non-NULL inputs.",
        arguments: &["inputs"],
        example: None,
    }],
    functions: &[
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &SimpleUnaryAggregate::new(&SumF64),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int64),
            &SimpleUnaryAggregate::new(&SumInt::<PhysicalI8>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int64),
            &SimpleUnaryAggregate::new(&SumInt::<PhysicalI16>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int64),
            &SimpleUnaryAggregate::new(&SumInt::<PhysicalI32>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64), // TODO: Return should be big num
            &SimpleUnaryAggregate::new(&SumInt::<PhysicalI64>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal64], DataTypeId::Decimal128),
            &SimpleUnaryAggregate::new(&SumDecimal::<Decimal64Type, Decimal128Type>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal128], DataTypeId::Decimal128),
            &SimpleUnaryAggregate::new(&SumDecimal::<Decimal128Type, Decimal128Type>::new()),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct SumInt<I: ScalarStorage> {
    _s: PhantomData<I>,
}

impl<I> SumInt<I>
where
    I: ScalarStorage,
{
    pub const fn new() -> Self {
        SumInt { _s: PhantomData }
    }
}

impl<I> UnaryAggregate for SumInt<I>
where
    I: ScalarStorage,
    I::StorageType: Sized + Default + AsPrimitive<i64>,
{
    type Input = I;
    type Output = PhysicalI64;

    type BindState = ();
    type GroupState = SumStateCheckedAdd<i64, I::StorageType>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SumF64;

impl UnaryAggregate for SumF64 {
    type Input = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = SumStateAdd<f64>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::float64(),
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SumDecimal<D1, D2> {
    _d1: PhantomData<D1>,
    _d2: PhantomData<D2>,
}

impl<D1, D2> SumDecimal<D1, D2> {
    pub const fn new() -> Self {
        SumDecimal {
            _d1: PhantomData,
            _d2: PhantomData,
        }
    }
}

impl<D1, D2> UnaryAggregate for SumDecimal<D1, D2>
where
    D1: DecimalType,
    D2: DecimalType,
    D1::Primitive: AsPrimitive<D2::Primitive>,
{
    type Input = D1::Storage;
    type Output = D2::Storage;

    type BindState = ();
    type GroupState = SumStateCheckedAdd<D2::Primitive, D1::Primitive>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        // Sum decimal always returns a 128-bit decimal with max precision. Get
        // the scale from the input.
        let d_meta = inputs[0].datatype()?.try_get_decimal_type_meta()?;
        let datatype =
            D2::datatype_from_decimal_meta(DecimalTypeMeta::new(D2::MAX_PRECISION, d_meta.scale));

        Ok(BindState {
            state: (),
            return_type: datatype,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

#[derive(Debug, Default)]
pub struct SumStateCheckedAdd<S, I> {
    sum: S,
    valid: bool,
    _input: PhantomData<I>,
}

impl<S, I> AggregateState<&I, S> for SumStateCheckedAdd<S, I>
where
    S: CheckedAdd + Default + Debug + Copy + Sync + Send + 'static,
    I: AsPrimitive<S> + Debug + Sync + Send,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.sum = self.sum.checked_add(&other.sum).unwrap_or_default(); // TODO
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, _state: &(), input: &I) -> Result<()> {
        self.sum = self.sum.checked_add(&input.as_()).unwrap_or_default(); // TODO
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = S>,
    {
        if self.valid {
            output.put(&self.sum);
        } else {
            output.put_null();
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SumStateAdd<T> {
    sum: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for SumStateAdd<T>
where
    T: AddAssign + Default + Debug + Copy + Sync + Send,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, _state: &(), &input: &T) -> Result<()> {
        self.sum += input;
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.sum);
        } else {
            output.put_null();
        }
        Ok(())
    }
}
