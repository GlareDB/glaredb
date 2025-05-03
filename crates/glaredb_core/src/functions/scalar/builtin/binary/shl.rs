use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
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
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_SHL: ScalarFunctionSet = ScalarFunctionSet {
    name: "shl",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Shifts an integer left by a specified number of bits.",
        arguments: &["value", "shift"],
        example: Some(Example {
            example: "shl(4, 1)",
            output: "8",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int32], DataTypeId::Int8),
            &Shl::<PhysicalI8>::new(&DataType::Int8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int32], DataTypeId::Int16),
            &Shl::<PhysicalI16>::new(&DataType::Int16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Shl::<PhysicalI32>::new(&DataType::Int32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int32], DataTypeId::Int64),
            &Shl::<PhysicalI64>::new(&DataType::Int64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int128, DataTypeId::Int32], DataTypeId::Int128),
            &Shl::<PhysicalI128>::new(&DataType::Int128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::Int32], DataTypeId::UInt8),
            &Shl::<PhysicalU8>::new(&DataType::UInt8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt16, DataTypeId::Int32], DataTypeId::UInt16),
            &Shl::<PhysicalU16>::new(&DataType::UInt16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt32, DataTypeId::Int32], DataTypeId::UInt32),
            &Shl::<PhysicalU32>::new(&DataType::UInt32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt64, DataTypeId::Int32], DataTypeId::UInt64),
            &Shl::<PhysicalU64>::new(&DataType::UInt64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::Int32],
                DataTypeId::UInt128,
            ),
            &Shl::<PhysicalU128>::new(&DataType::UInt128),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Shl<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Shl<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Shl {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Shl<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Shl<i32, Output = S::StorageType> + Sized + Copy,
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

        BinaryExecutor::execute::<S, PhysicalI32, S, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| {
                let type_bits = std::mem::size_of::<S::StorageType>() * 8;
                if b >= type_bits as i32 {
                    buf.put(&(S::StorageType::default()))
                } else {
                    buf.put(&(a << b))
                }
            },
        )
    }
}
