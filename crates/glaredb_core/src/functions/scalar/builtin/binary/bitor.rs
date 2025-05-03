use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::BitOr;

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

pub const FUNCTION_SET_BITOR: ScalarFunctionSet = ScalarFunctionSet {
    name: "bitor",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Performs a bitwise OR operation on two integers.",
        arguments: &["value1", "value2"],
        example: Some(Example {
            example: "bitor(5, 2)",
            output: "7",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &BitOr_::<PhysicalI8>::new(&DataType::Int8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &BitOr_::<PhysicalI16>::new(&DataType::Int16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &BitOr_::<PhysicalI32>::new(&DataType::Int32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &BitOr_::<PhysicalI64>::new(&DataType::Int64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int128, DataTypeId::Int128], DataTypeId::Int128),
            &BitOr_::<PhysicalI128>::new(&DataType::Int128),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &BitOr_::<PhysicalU8>::new(&DataType::UInt8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt16, DataTypeId::UInt16], DataTypeId::UInt16),
            &BitOr_::<PhysicalU16>::new(&DataType::UInt16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt32, DataTypeId::UInt32], DataTypeId::UInt32),
            &BitOr_::<PhysicalU32>::new(&DataType::UInt32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::UInt64, DataTypeId::UInt64], DataTypeId::UInt64),
            &BitOr_::<PhysicalU64>::new(&DataType::UInt64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &BitOr_::<PhysicalU128>::new(&DataType::UInt128),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct BitOr_<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> BitOr_<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        BitOr_ {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for BitOr_<S>
where
    S: MutableScalarStorage,
    S::StorageType:
        BitOr<Output = S::StorageType> + Default + Sized + Copy,
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
            |&a, &b, buf| buf.put(&(a | b)),
        )
    }
}
