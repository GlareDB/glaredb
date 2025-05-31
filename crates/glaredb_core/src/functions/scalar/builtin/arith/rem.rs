use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage, PhysicalF16, PhysicalF32, PhysicalF64, PhysicalI8, PhysicalI16,
    PhysicalI32, PhysicalI64, PhysicalI128, PhysicalU8, PhysicalU16, PhysicalU32, PhysicalU64,
    PhysicalU128,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_REM: ScalarFunctionSet = ScalarFunctionSet {
    name: "%",
    aliases: &["rem"],
    doc: &[&Documentation {
        category: Category::NUMERIC_OPERATOR,
        description: "Returns the remainder after dividing the left value by the right value.",
        arguments: &["left", "right"],
        example: Some(Example {
            example: "10 % 3",
            output: "1",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            &Rem::<PhysicalF16>::new(DataType::FLOAT16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            &Rem::<PhysicalF32>::new(DataType::FLOAT32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Rem::<PhysicalF64>::new(DataType::FLOAT64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Rem::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Rem::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Rem::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Rem::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Rem::<PhysicalI128>::new(DataType::INT128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &Rem::<PhysicalU8>::new(DataType::UINT8),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &Rem::<PhysicalU16>::new(DataType::UINT16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &Rem::<PhysicalU32>::new(DataType::UINT32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &Rem::<PhysicalU64>::new(DataType::UINT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &Rem::<PhysicalU128>::new(DataType::UINT128),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Rem<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Rem<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Rem {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Rem<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Rem<Output = S::StorageType> + Sized + Copy,
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
            |&a, &b, buf| buf.put(&(a % b)),
        )
    }
}
