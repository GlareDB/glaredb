use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::{DbError, Result};
use num_traits::{NumCast, PrimInt};

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
    PhysicalInterval,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    ScalarStorage,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::arrays::scalar::interval::Interval;
use crate::expr::{self, Expression};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_MUL: ScalarFunctionSet = ScalarFunctionSet {
    name: "*",
    aliases: &["mul"],
    doc: &[&Documentation {
        category: Category::NUMERIC_OPERATOR,
        description: "Multiplies two numeric values.",
        arguments: &["left", "right"],
        example: Some(Example {
            example: "5 * 3",
            output: "15",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            &Mul::<PhysicalF16>::new(DataType::FLOAT16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            &Mul::<PhysicalF32>::new(DataType::FLOAT32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Mul::<PhysicalF64>::new(DataType::FLOAT64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Mul::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Mul::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Mul::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Mul::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Mul::<PhysicalI128>::new(DataType::INT128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &Mul::<PhysicalU8>::new(DataType::UINT8),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &Mul::<PhysicalU16>::new(DataType::UINT16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &Mul::<PhysicalU32>::new(DataType::UINT32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &Mul::<PhysicalU64>::new(DataType::UINT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &Mul::<PhysicalU128>::new(DataType::UINT128),
        ),
        // Decimal64
        RawScalarFunction::new(D_SIGS.d64_d64, &DecimalMul::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i8, &DecimalMul::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i16, &DecimalMul::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.d64_i32, &DecimalMul::<Decimal64Type>::new()),
        // RawScalarFunction::new(D_SIGS.d64_i64, &DecimalMul::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i8_d64, &DecimalMul::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i16_d64, &DecimalMul::<Decimal64Type>::new()),
        RawScalarFunction::new(D_SIGS.i32_d64, &DecimalMul::<Decimal64Type>::new()),
        // RawScalarFunction::new(D_SIGS.i64_d64, &DecimalMul::<Decimal64Type>::new()),
        // Decimal128
        RawScalarFunction::new(D_SIGS.d128_d128, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i8, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i16, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i32, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.d128_i64, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i8_d128, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i16_d128, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i32_d128, &DecimalMul::<Decimal128Type>::new()),
        RawScalarFunction::new(D_SIGS.i64_d128, &DecimalMul::<Decimal128Type>::new()),
        // interval * int
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Interval, DataTypeId::Int32],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI32, false>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Interval, DataTypeId::Int64],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI64, false>::new(),
        ),
        // int * interval
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int32, DataTypeId::Interval],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI32, true>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int64, DataTypeId::Interval],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI64, true>::new(),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Mul<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Mul<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Mul {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Mul<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Mul<Output = S::StorageType> + Sized + Copy,
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
            |&a, &b, buf| buf.put(&(a * b)),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalMul<D> {
    _d: PhantomData<D>,
}

impl<D> DecimalMul<D> {
    pub const fn new() -> Self {
        DecimalMul { _d: PhantomData }
    }
}

impl<D> ScalarFunction for DecimalMul<D>
where
    D: DecimalType,
{
    type State = ();

    fn bind(&self, mut inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let mut right = inputs.pop().unwrap();
        let mut left = inputs.pop().unwrap();

        let l_type = left.datatype()?;
        let r_type = right.datatype()?;

        let (cast_left, l_meta) = match D::decimal_meta_opt(&l_type) {
            Some(meta) => (false, meta),
            None => {
                let meta = DecimalTypeMeta::new_for_datatype_id(l_type.id()).ok_or_else(|| {
                    DbError::new(format!("Cannot convert {l_type} into a decimal"))
                })?;
                (true, meta)
            }
        };

        let (cast_right, r_meta) = match D::decimal_meta_opt(&r_type) {
            Some(meta) => (false, meta),
            None => {
                let meta = DecimalTypeMeta::new_for_datatype_id(r_type.id()).ok_or_else(|| {
                    DbError::new(format!("Cannot convert {r_type} into a decimal"))
                })?;
                (true, meta)
            }
        };

        let mut new_precision = l_meta.precision + r_meta.precision;
        let new_scale = l_meta.scale + r_meta.scale;

        if new_scale > D::MAX_PRECISION as i8 {
            return Err(DbError::new(format!(
                "Resuling decimal scale of '{new_scale}' exceeds max decimal precion '{}'",
                D::MAX_PRECISION
            )));
        }

        if new_precision > D::MAX_PRECISION {
            // TODO: Need to check overflow.
            new_precision = D::MAX_PRECISION;
        }

        if new_scale > new_precision as i8 {
            return Err(DbError::new(format!(
                "Compute scale '{new_scale}' exceeds computed precision '{new_precision}'"
            )));
        }

        let return_type = D::datatype_from_decimal_meta(DecimalTypeMeta {
            precision: new_precision,
            scale: new_scale,
        });

        // We only need to cast the left/right inputs if they're not already
        // decimals.
        //
        // Note we don't cast to the return type, we cast to the original
        // decimal metas for the type.
        if cast_left {
            let left_type = D::datatype_from_decimal_meta(l_meta);
            left = expr::cast(left, left_type)?.into();
        }

        if cast_right {
            let right_type = D::datatype_from_decimal_meta(r_meta);
            right = expr::cast(right, right_type)?.into();
        }

        Ok(BindState {
            state: (),
            return_type,
            inputs: vec![left, right],
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryExecutor::execute::<D::Storage, D::Storage, D::Storage, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| buf.put(&(a * b)),
        )
    }
}

/// Multiply interval with integer (rhs).
#[derive(Debug, Clone, Copy)]
pub struct MulInterval<Rhs, const LHS_RHS_FLIPPED: bool> {
    _rhs: PhantomData<Rhs>,
}

impl<Rhs, const LHS_RHS_FLIPPED: bool> MulInterval<Rhs, LHS_RHS_FLIPPED> {
    pub const fn new() -> Self {
        MulInterval { _rhs: PhantomData }
    }
}

impl<Rhs, const LHS_RHS_FLIPPED: bool> ScalarFunction for MulInterval<Rhs, LHS_RHS_FLIPPED>
where
    Rhs: ScalarStorage,
    Rhs::StorageType: PrimInt,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::interval(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        let (lhs, rhs) = if LHS_RHS_FLIPPED { (b, a) } else { (a, b) };

        BinaryExecutor::execute::<PhysicalInterval, Rhs, PhysicalInterval, _>(
            lhs,
            sel,
            rhs,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| {
                // TODO: Overflow check
                buf.put(&Interval {
                    months: a.months * (<i32 as NumCast>::from(b).unwrap_or_default()),
                    days: a.days * (<i32 as NumCast>::from(b).unwrap_or_default()),
                    nanos: a.nanos * (<i64 as NumCast>::from(b).unwrap_or_default()),
                })
            },
        )
    }
}
