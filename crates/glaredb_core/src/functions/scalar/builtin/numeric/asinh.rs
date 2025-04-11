use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;
use num_traits::Float;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_ASINH: ScalarFunctionSet = ScalarFunctionSet {
    name: "asinh",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute the inverse hyperbolic sine of value.",
        arguments: &["float"],
        example: None,
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &Asinh::<PhysicalF16>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &Asinh::<PhysicalF32>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &Asinh::<PhysicalF64>::new(&DataType::Float64),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Asinh<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Asinh<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Asinh {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Asinh<S>
where
    S: MutableScalarStorage,
    S::StorageType: Float,
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
        let val = &input.arrays()[0];

        UnaryExecutor::execute::<S, S, _>(val, sel, OutBuffer::from_array(output)?, |&v, buf| {
            buf.put(&v.asinh())
        })
    }
}
