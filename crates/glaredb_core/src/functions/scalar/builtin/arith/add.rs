use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

use super::decimal_arith::common_add_sub_decimal_type_info;
use super::decimal_sigs::D_SIGS;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::expr::{self, Expression};
use crate::functions::Signature;
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_ADD: ScalarFunctionSet = ScalarFunctionSet {
    name: "+",
    aliases: &["add"],
    doc: &[],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            &Add::<PhysicalF16>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            &Add::<PhysicalF32>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Add::<PhysicalF64>::new(&DataType::Float64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Add::<PhysicalI8>::new(&DataType::Int8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Add::<PhysicalI16>::new(&DataType::Int16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Add::<PhysicalI32>::new(&DataType::Int32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Add::<PhysicalI64>::new(&DataType::Int64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Add::<PhysicalI128>::new(&DataType::Int128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &Add::<PhysicalU8>::new(&DataType::UInt8),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &Add::<PhysicalU16>::new(&DataType::UInt16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &Add::<PhysicalU32>::new(&DataType::UInt32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &Add::<PhysicalU64>::new(&DataType::UInt64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &Add::<PhysicalU128>::new(&DataType::UInt128),
        ),
        // Decimal64
        RawScalarFunction::new(D_SIGS.d64_d64, &DecimalAdd::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i8, &DecimalAdd::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i16, &DecimalAdd::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i32, &DecimalAdd::<Decimal64Type>::new()),
        // RawScalarFunction::new(D_SIGS.d64_i64, &DecimalAdd::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i8_d64, &DecimalAdd::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i16_d64, &DecimalAdd::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i32_d64, &DecimalAdd::<Decimal64Type>::new()),
        // RawScalarFunction::new(D_SIGS.i64_d64, &DecimalAdd::<Decimal64Type>::new()),
        // Decimal128
        RawScalarFunction::new(D_SIGS.d128_d128, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i8, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i16, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i32, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i64, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i8_d128, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i16_d128, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i32_d128, &DecimalAdd::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i64_d128, &DecimalAdd::<Decimal128Type>::new()),
        // Date + days => date
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Date32, DataTypeId::Int32], DataTypeId::Date32),
            &Add::<PhysicalI32>::new(&DataType::Date32),
        ),
        // Days + date => date
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Date32], DataTypeId::Date32),
            &Add::<PhysicalI32>::new(&DataType::Date32),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Add<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Add<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Add {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Add<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Add<Output = S::StorageType> + Sized + Copy,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: self.return_type.clone(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, S, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| buf.put(&(a + b)),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalAdd<D> {
    _d: PhantomData<D>,
}

impl<D> DecimalAdd<D>
where
    D: DecimalType,
{
    pub const fn new() -> Self {
        DecimalAdd { _d: PhantomData }
    }
}

impl<D> ScalarFunction for DecimalAdd<D>
where
    D: DecimalType,
{
    type State = ();

    fn bind(&self, mut inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let mut right = inputs.pop().unwrap();
        let mut left = inputs.pop().unwrap();

        let l_type = left.datatype()?;
        let r_type = right.datatype()?;

        let info = common_add_sub_decimal_type_info::<D>(&l_type, &r_type)?;

        let return_type = D::datatype_from_decimal_meta(info.meta);

        // Cast the inputs if needed.
        match D::decimal_meta_opt(&l_type) {
            Some(meta) if meta == info.meta => (), // Nothing to do.
            _ => left = expr::cast(left, return_type.clone()).into(),
        }

        match D::decimal_meta_opt(&r_type) {
            Some(meta) if meta == info.meta => (), // Nothing to do.
            _ => right = expr::cast(right, return_type.clone()).into(),
        }

        Ok(BindState {
            state: (),
            return_type,
            inputs: vec![left, right],
        })
    }

    fn execute(_: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryExecutor::execute::<D::Storage, D::Storage, D::Storage, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| buf.put(&(a + b)),
        )
    }
}
