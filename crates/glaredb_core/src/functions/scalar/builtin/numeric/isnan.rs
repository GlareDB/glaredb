use std::marker::PhantomData;

use glaredb_error::Result;
use num_traits::Float;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    PhysicalBool, PhysicalF16, PhysicalF32, PhysicalF64, ScalarStorage,
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

pub const FUNCTION_SET_IS_NAN: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_nan",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Return if the given float is a NaN.",
        arguments: &["float"],
        example: Some(Example {
            example: "isnan('NaN'::FLOAT)",
            output: "true",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Boolean),
            &IsNan::<PhysicalF16>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Boolean),
            &IsNan::<PhysicalF32>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Boolean),
            &IsNan::<PhysicalF64>::new(),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct IsNan<S: ScalarStorage> {
    _s: PhantomData<S>,
}

impl<S: ScalarStorage> IsNan<S> {
    pub const fn new() -> Self {
        IsNan { _s: PhantomData }
    }
}

impl<S> ScalarFunction for IsNan<S>
where
    S: ScalarStorage,
    S::StorageType: Float,
{
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
        let input = &input.arrays()[0];

        UnaryExecutor::execute::<S, PhysicalBool, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |&v, buf| buf.put(&v.is_nan()),
        )
    }
}
