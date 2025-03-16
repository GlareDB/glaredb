use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;

pub const FUNCTION_SET_NEGATE: ScalarFunctionSet = ScalarFunctionSet {
    name: "negate",
    aliases: &[],
    doc: None,
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &Negate::<PhysicalF16>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &Negate::<PhysicalF32>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &Negate::<PhysicalF64>::new(&DataType::Float64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
            &Negate::<PhysicalI8>::new(&DataType::Int8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int16),
            &Negate::<PhysicalI16>::new(&DataType::Int16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int32),
            &Negate::<PhysicalI32>::new(&DataType::Int32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
            &Negate::<PhysicalI64>::new(&DataType::Int64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int128], DataTypeId::Int128),
            &Negate::<PhysicalI128>::new(&DataType::Int128),
        ),
    ],
};

pub const FUNCTION_SET_NOT: ScalarFunctionSet = ScalarFunctionSet {
    name: "not",
    aliases: &[],
    doc: None,
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &Not,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Negate<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Negate<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Negate {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Negate<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Neg<Output = S::StorageType> + Copy,
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
            |&a, buf| buf.put(&(-a)),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Not;

impl ScalarFunction for Not {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        UnaryExecutor::execute::<PhysicalBool, PhysicalBool, _>(
            &input.arrays()[0],
            sel,
            OutBuffer::from_array(output)?,
            |&b, buf| buf.put(&(!b)),
        )
    }
}
