use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

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
use crate::expr::{self, Expression};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_DIV: ScalarFunctionSet = ScalarFunctionSet {
    name: "/",
    aliases: &["div"],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Divides the left value by the right value.",
        arguments: &["left", "right"],
        example: Some(Example {
            example: "15 / 3",
            output: "5",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            &Div::<PhysicalF16>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            &Div::<PhysicalF32>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Div::<PhysicalF64>::new(&DataType::Float64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Div::<PhysicalI8>::new(&DataType::Int8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Div::<PhysicalI16>::new(&DataType::Int16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Div::<PhysicalI32>::new(&DataType::Int32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Div::<PhysicalI64>::new(&DataType::Int64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Div::<PhysicalI128>::new(&DataType::Int128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &Div::<PhysicalU8>::new(&DataType::UInt8),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &Div::<PhysicalU16>::new(&DataType::UInt16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &Div::<PhysicalU32>::new(&DataType::UInt32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &Div::<PhysicalU64>::new(&DataType::UInt64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &Div::<PhysicalU128>::new(&DataType::UInt128),
        ),
        // Decimals
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                DataTypeId::Float64,
            ),
            &DivDecimal,
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Decimal128, DataTypeId::Decimal128],
                DataTypeId::Float64,
            ),
            &DivDecimal,
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Div<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Div<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Div {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Div<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Div<Output = S::StorageType> + Sized + Copy,
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
            |&a, &b, buf| buf.put(&(a / b)),
        )
    }
}

/// Current implementation just casts both side to float64.
#[derive(Debug, Clone, Copy)]
pub struct DivDecimal;

impl ScalarFunction for DivDecimal {
    type State = ();

    fn bind(&self, mut inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        // Wrap decimals in float casts. Then we'll just do the div on floats.
        debug_assert_eq!(2, inputs.len());
        let right = expr::cast(inputs.pop().unwrap(), DataType::Float64)?.into();
        let left = expr::cast(inputs.pop().unwrap(), DataType::Float64)?.into();

        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs: vec![left, right],
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryExecutor::execute::<PhysicalF64, PhysicalF64, PhysicalF64, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| buf.put(&(a / b)),
        )
    }
}
