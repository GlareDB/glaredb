use std::marker::PhantomData;

use glaredb_error::Result;
use num_traits::Float;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalF64};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_ATAN2: ScalarFunctionSet = ScalarFunctionSet {
    name: "atan2",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute the arctangent of y/x.",
        arguments: &["y", "x"],
        example: None,
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &Atan2::<PhysicalF64>::new(DataType::FLOAT64),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Atan2<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Atan2<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Atan2 {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Atan2<S>
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
        let y = &input.arrays()[0];
        let x = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, S, _>(
            y,
            sel,
            x,
            sel,
            OutBuffer::from_array(output)?,
            |&y, &x, buf| buf.put(&y.atan2(x)),
        )
    }
}
