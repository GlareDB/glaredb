use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;
use num_traits::Float;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF64,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_POWER: ScalarFunctionSet = ScalarFunctionSet {
    name: "power",
    aliases: &["pow"],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute base raised to the power of exponent",
        arguments: &["base", "exponent"],
        example: None,
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Power::<PhysicalF64>::new(&DataType::Float64),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Power<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Power<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Power {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Power<S>
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
        let base = &input.arrays()[0];
        let exponent = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, S, _>(
            base,
            sel,
            exponent,
            sel,
            OutBuffer::from_array(output)?,
            |&base, &exponent, buf| buf.put(&base.powf(exponent)),
        )
    }
}
