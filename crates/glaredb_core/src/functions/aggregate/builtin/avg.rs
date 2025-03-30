use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;

use glaredb_error::Result;
use num_traits::AsPrimitive;

use crate::arrays::array::physical_type::{
    AddressableMut,
    PhysicalF64,
    PhysicalI64,
    ScalarStorage,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;

pub const FUNCTION_SET_AVG: AggregateFunctionSet = AggregateFunctionSet {
    name: "avg",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the average value from the input column.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal64], DataTypeId::Float64),
            &SimpleUnaryAggregate::new(&AvgDecimal::<Decimal64Type>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Decimal128], DataTypeId::Float64),
            &SimpleUnaryAggregate::new(&AvgDecimal::<Decimal128Type>::new()),
        ),
        // i64 uses an i128 as its intermediate sum to avoid (delay) overflow
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Float64), // TODO: Should be decimal // TODO: Should it though?
            &SimpleUnaryAggregate::new(&Avg::<PhysicalI64, i128>::new()),
        ),
        RawAggregateFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &SimpleUnaryAggregate::new(&Avg::<PhysicalF64, f64>::new()),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Avg<S, T> {
    _s: PhantomData<S>,
    _t: PhantomData<T>,
}

impl<S, T> Avg<S, T> {
    pub const fn new() -> Self {
        Avg {
            _s: PhantomData,
            _t: PhantomData,
        }
    }
}

impl<S, T> UnaryAggregate for Avg<S, T>
where
    S: ScalarStorage,
    S::StorageType: Into<T> + Copy + Default + Debug + Sync + Send,
    T: AsPrimitive<f64> + AddAssign + Debug + Default + Sync + Send,
{
    type Input = S;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = AvgStateF64<S::StorageType, T>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

#[derive(Debug)]
pub struct AvgDecimalBindState {
    scale: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct AvgDecimal<D> {
    _d: PhantomData<D>,
}

impl<D> AvgDecimal<D> {
    pub const fn new() -> Self {
        AvgDecimal { _d: PhantomData }
    }
}

impl<D> UnaryAggregate for AvgDecimal<D>
where
    D: DecimalType,
    D::Primitive: Into<i128>,
{
    type Input = D::Storage;
    type Output = PhysicalF64;

    type BindState = AvgDecimalBindState;
    type GroupState = AvgStateDecimal<D::Primitive>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        let datatype = inputs[0].datatype()?;
        let m = datatype.try_get_decimal_type_meta()?;

        let scale = f64::powi(10.0, m.scale.abs() as i32);

        Ok(BindState {
            state: AvgDecimalBindState { scale },
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn new_aggregate_state(state: &Self::BindState) -> Self::GroupState {
        AvgStateDecimal::<D::Primitive> {
            scale: state.scale,
            sum: 0,
            count: 0,
            _input: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AvgStateDecimal<I> {
    /// Scale to use when finalizing the physical decimal value.
    scale: f64,
    sum: i128,
    count: i64,
    _input: PhantomData<I>,
}

impl<I> AggregateState<&I, f64> for AvgStateDecimal<I>
where
    I: Into<i128> + Copy + Debug + Sync + Send,
{
    type BindState = AvgDecimalBindState;

    fn merge(&mut self, _state: &Self::BindState, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _state: &Self::BindState, &input: &I) -> Result<()> {
        self.sum += input.into();
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &Self::BindState, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        if self.count == 0 {
            output.put_null();
            return Ok(());
        }

        let val = (self.sum as f64) / (self.count as f64 * self.scale);
        output.put(&val);

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct AvgStateF64<I, T> {
    sum: T,
    count: i64,
    _input: PhantomData<I>,
}

impl<I, T> AggregateState<&I, f64> for AvgStateF64<I, T>
where
    I: Into<T> + Copy + Default + Debug + Sync + Send,
    T: AsPrimitive<f64> + AddAssign + Debug + Default + Sync + Send,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _state: &(), &input: &I) -> Result<()> {
        self.sum += input.into();
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        if self.count == 0 {
            output.put_null();
            return Ok(());
        }
        let sum: f64 = self.sum.as_();
        let val = sum / self.count as f64;
        output.put(&val);

        Ok(())
    }
}
