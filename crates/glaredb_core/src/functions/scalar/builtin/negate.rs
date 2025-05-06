use std::marker::PhantomData;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
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

pub const FUNCTION_SET_NEGATE: ScalarFunctionSet = ScalarFunctionSet {
    name: "negate",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Returns the result of multiplying the input value by -1.",
        arguments: &["value"],
        example: Some(Example {
            example: "negate(-3.5)",
            output: "3.5",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &Negate::<PhysicalF16>::new(DataType::FLOAT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &Negate::<PhysicalF32>::new(DataType::FLOAT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &Negate::<PhysicalF64>::new(DataType::FLOAT64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
            &Negate::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16], DataTypeId::Int16),
            &Negate::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32], DataTypeId::Int32),
            &Negate::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
            &Negate::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int128], DataTypeId::Int128),
            &Negate::<PhysicalI128>::new(DataType::INT128),
        ),
    ],
};

pub const FUNCTION_SET_NOT: ScalarFunctionSet = ScalarFunctionSet {
    name: "not",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Returns TRUE if the input is FALSE, and FALSE if the input is TRUE.",
        arguments: &["value"],
        example: Some(Example {
            example: "not(TRUE)",
            output: "FALSE",
        }),
    }],
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
            return_type: DataType::boolean(),
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
