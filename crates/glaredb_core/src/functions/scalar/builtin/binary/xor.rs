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
use crate::functions::function_set::{FnName, ScalarFunctionSet};
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_XOR: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("xor"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Performs a bitwise XOR operation on two integers.",
        arguments: &["value1", "value2"],
        example: Some(Example {
            example: "xor(5, 3)",
            output: "6",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &BitXor::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &BitXor::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &BitXor::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &BitXor::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &BitXor::<PhysicalI128>::new(DataType::INT128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &BitXor::<PhysicalU8>::new(DataType::UINT8),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &BitXor::<PhysicalU16>::new(DataType::UINT16),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &BitXor::<PhysicalU32>::new(DataType::UINT32),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &BitXor::<PhysicalU64>::new(DataType::UINT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &BitXor::<PhysicalU128>::new(DataType::UINT128),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct BitXor<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> BitXor<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        BitXor {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for BitXor<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::BitXor<Output = S::StorageType> + Default + Sized + Copy,
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
            |&a, &b, buf| buf.put(&(a ^ b)),
        )
    }
}
