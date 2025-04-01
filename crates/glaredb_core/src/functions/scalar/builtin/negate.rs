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
        signature: "negate(x) -> same as input",
        summary: "Returns the negation of the input value.",
        description: "Returns the result of multiplying the input value by -1.",
        category: Category::Math,
        examples: &[
            Example {
                sql: "SELECT negate(5)",
                result: "-5",
            },
            Example {
                sql: "SELECT negate(-3.5)",
                result: "3.5",
            },
        ],
    }],
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
    doc: &[&Documentation {
        signature: "not(x) -> boolean",
        summary: "Returns the logical negation of the input value.",
        description: "Returns TRUE if the input is FALSE, and FALSE if the input is TRUE.",
        category: Category::Logical,
        examples: &[
            Example {
                sql: "SELECT not(TRUE)",
                result: "FALSE",
            },
            Example {
                sql: "SELECT not(FALSE)",
                result: "TRUE",
            },
        ],
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
