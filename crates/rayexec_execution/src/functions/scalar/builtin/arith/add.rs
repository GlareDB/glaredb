use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::{OptionExt, RayexecError, Result};

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
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::expr::Expression;
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;

pub const FUNCTION_SET_ADD: ScalarFunctionSet = ScalarFunctionSet {
    name: "+",
    aliases: &["add"],
    doc: None,
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
        // Decimals.
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                DataTypeId::Decimal64,
            ),
            &AddDecimal::<Decimal64Type>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Decimal128, DataTypeId::Decimal128],
                DataTypeId::Decimal128,
            ),
            &AddDecimal::<Decimal128Type>::new(),
        ),
    ],
};

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct AddDecimal<D> {
    _d: PhantomData<D>,
}

impl<S> AddDecimal<S> {
    pub const fn new() -> Self {
        AddDecimal { _d: PhantomData }
    }
}

impl<D> ScalarFunction for AddDecimal<D>
where
    D: DecimalType,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let d1 = inputs[0].datatype()?;
        let d2 = inputs[1].datatype()?;

        let m1 = D::try_unwrap_decimal_meta(&d1).required("Incorrect decimal type")?;
        let m2 = D::try_unwrap_decimal_meta(&d2).required("Incorrect decimal type")?;

        // TODO: We could add a cast here.
        //
        // But hopefully we're consistently casting types where needed.
        if m1.precision != m2.precision || m1.scale != m2.scale {
            return Err(RayexecError::new(
                "Decimal types have different precision/scale",
            ));
        }

        Ok(BindState {
            state: (),
            return_type: D::datatype_from_decimal_meta(m1),
            inputs,
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
            |&a, &b, buf| buf.put(&(a + b)),
        )
    }
}
