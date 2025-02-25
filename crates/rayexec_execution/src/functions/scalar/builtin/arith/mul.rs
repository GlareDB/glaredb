use std::fmt::Debug;
use std::marker::PhantomData;

use num_traits::{NumCast, PrimInt};
use rayexec_error::{OptionExt, Result};

use crate::arrays::array::physical_type::{
    MutableScalarStorage,
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
    ScalarStorage,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::arrays::scalar::interval::Interval;
use crate::expr::Expression;
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;

pub const FUNCTION_SET_MUL: ScalarFunctionSet = ScalarFunctionSet {
    name: "*",
    aliases: &["mul"],
    doc: None,
    functions: &[
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            &Mul::<PhysicalF16>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            &Mul::<PhysicalF32>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Mul::<PhysicalF64>::new(&DataType::Float64),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Mul::<PhysicalI8>::new(&DataType::Int8),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Mul::<PhysicalI16>::new(&DataType::Int16),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Mul::<PhysicalI32>::new(&DataType::Int32),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Mul::<PhysicalI64>::new(&DataType::Int64),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Mul::<PhysicalI128>::new(&DataType::Int128),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &Mul::<PhysicalU8>::new(&DataType::UInt8),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &Mul::<PhysicalU16>::new(&DataType::UInt16),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &Mul::<PhysicalU32>::new(&DataType::UInt32),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &Mul::<PhysicalU64>::new(&DataType::UInt64),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &Mul::<PhysicalU128>::new(&DataType::UInt128),
        ),
        // Decimals.
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                DataTypeId::Decimal64,
            ),
            &MulDecimal::<Decimal64Type>::new(),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Decimal128, DataTypeId::Decimal128],
                DataTypeId::Decimal128,
            ),
            &MulDecimal::<Decimal128Type>::new(),
        ),
        // interval * int
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Interval, DataTypeId::Int32],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI32, false>::new(),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Interval, DataTypeId::Int64],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI64, false>::new(),
        ),
        // int * interval
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Int32, DataTypeId::Interval],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI32, true>::new(),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Int64, DataTypeId::Interval],
                DataTypeId::Interval,
            ),
            &MulInterval::<PhysicalI64, true>::new(),
        ),
    ],
};

#[derive(Debug, Clone)]
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

    fn execute(&self, _state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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

#[derive(Debug, Clone)]
pub struct MulDecimal<D> {
    _d: PhantomData<D>,
}

impl<D> MulDecimal<D> {
    pub const fn new() -> Self {
        MulDecimal { _d: PhantomData }
    }
}

impl<D> ScalarFunction for MulDecimal<D>
where
    D: DecimalType,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let d1 = inputs[0].datatype()?;
        let d2 = inputs[1].datatype()?;

        let m1 = D::try_unwrap_decimal_meta(&d1).required("Decimal data type")?;
        let m2 = D::try_unwrap_decimal_meta(&d2).required("Decimal data type")?;

        // Since we're multiplying, might as well go wide as possible.
        // Eventually we'll want to bumpt up to 128 if the precision is
        // over some threshold to be more resilient to overflows.
        let precision = D::MAX_PRECISION;
        let scale = m1.scale + m2.scale;
        let return_type = D::datatype_from_decimal_meta(DecimalTypeMeta::new(precision, scale));

        Ok(BindState {
            state: (),
            return_type,
            inputs,
        })
    }

    fn execute(&self, _state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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
#[derive(Debug, Clone)]
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
            return_type: DataType::Interval,
            inputs,
        })
    }

    fn execute(&self, _state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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
