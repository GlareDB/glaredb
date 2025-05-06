use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

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
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::builtin::arith::decimal_arith::common_add_sub_decimal_type_info;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_SUB: ScalarFunctionSet = ScalarFunctionSet {
    name: "-",
    aliases: &["sub"],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Subtracts the right value from the left value.",
        arguments: &["left", "right"],
        example: Some(Example {
            example: "10 - 4",
            output: "6",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            &Sub::<PhysicalF16>::new(DataType::FLOAT16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            &Sub::<PhysicalF32>::new(DataType::FLOAT32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Sub::<PhysicalF64>::new(DataType::FLOAT64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Sub::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Sub::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Sub::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Sub::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Sub::<PhysicalI128>::new(DataType::INT128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &Sub::<PhysicalU8>::new(DataType::UINT8),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &Sub::<PhysicalU16>::new(DataType::UINT16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &Sub::<PhysicalU32>::new(DataType::UINT32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &Sub::<PhysicalU64>::new(DataType::UINT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &Sub::<PhysicalU128>::new(DataType::UINT128),
        ),
        // Decimal64
        RawScalarFunction::new(D_SIGS.d64_d64, &DecimalSub::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i8, &DecimalSub::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i16, &DecimalSub::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i32, &DecimalSub::<Decimal64Type>::new()),
        // RawScalarFunction::new(D_SIGS.d64_i64, &DecimalSub::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i8_d64, &DecimalSub::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i16_d64, &DecimalSub::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i32_d64, &DecimalSub::<Decimal64Type>::new()),
        // RawScalarFunction::new(D_SIGS.i64_d64, &DecimalSub::<Decimal64Type>::new()),
        // Decimal128
        RawScalarFunction::new(D_SIGS.d128_d128, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i8, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i16, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i32, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i64, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i8_d128, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i16_d128, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i32_d128, &DecimalSub::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i64_d128, &DecimalSub::<Decimal128Type>::new()),
        // date - days
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Date32, DataTypeId::Int32], DataTypeId::Date32),
            &Sub::<PhysicalI32>::new(DataType::DATE32),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Sub<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Sub<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Sub {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Sub<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Sub<Output = S::StorageType> + Sized + Copy,
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
            |&a, &b, buf| buf.put(&(a - b)),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalSub<D> {
    _d: PhantomData<D>,
}

impl<D> DecimalSub<D>
where
    D: DecimalType,
{
    pub const fn new() -> Self {
        DecimalSub { _d: PhantomData }
    }
}

impl<D> ScalarFunction for DecimalSub<D>
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
            _ => left = expr::cast(left, return_type.clone())?.into(),
        }

        match D::decimal_meta_opt(&r_type) {
            Some(meta) if meta == info.meta => (), // Nothing to do.
            _ => right = expr::cast(right, return_type.clone())?.into(),
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
            |&a, &b, buf| buf.put(&(a - b)),
        )
    }
}
