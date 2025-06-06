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
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

// Note this should not be part of default function list to avoid potential
// conflict with the aggregate function.
pub const FUNCTION_SET_BITNOT: ScalarFunctionSet = ScalarFunctionSet {
    name: "bitnot",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Performs a bitwise NOT operation on an integer.",
        arguments: &["value"],
        example: Some(Example {
            example: "bitnot(5)",
            output: "-6",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
            &BitNot::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int16),
            &BitNot::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int32),
            &BitNot::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
            &BitNot::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int128], DataTypeId::Int128),
            &BitNot::<PhysicalI128>::new(DataType::INT128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8], DataTypeId::UInt8),
            &BitNot::<PhysicalU8>::new(DataType::UINT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt16], DataTypeId::UInt16),
            &BitNot::<PhysicalU16>::new(DataType::UINT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt32], DataTypeId::UInt32),
            &BitNot::<PhysicalU32>::new(DataType::UINT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt64], DataTypeId::UInt64),
            &BitNot::<PhysicalU64>::new(DataType::UINT64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt128], DataTypeId::UInt128),
            &BitNot::<PhysicalU128>::new(DataType::UINT128),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct BitNot<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> BitNot<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        BitNot {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for BitNot<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Not<Output = S::StorageType> + Copy,
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

        UnaryExecutor::execute::<S, S, _>(
            &input.arrays()[0],
            sel,
            OutBuffer::from_array(output)?,
            |&a, buf| buf.put(&(!a)),
        )
    }
}
