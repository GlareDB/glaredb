use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::PhysicalF64;
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::{DataType, DataTypeId};
use glaredb_core::arrays::executor::OutBuffer;
use glaredb_core::arrays::executor::scalar::UnaryExecutor;
use glaredb_core::expr::Expression;
use glaredb_core::functions::Signature;
use glaredb_core::functions::bind_state::BindState;
use glaredb_core::functions::documentation::{Category, Documentation};
use glaredb_core::functions::function_set::ScalarFunctionSet;
use glaredb_core::functions::scalar::{RawScalarFunction, ScalarFunction};
use glaredb_error::Result;

pub const FUNCTION_SET_EXPM1: ScalarFunctionSet = ScalarFunctionSet {
    name: "expm1",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Computes the exponential of the given value minus one.",
        arguments: &["value"],
        example: None,
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
        &Expm1,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Expm1;

impl ScalarFunction for Expm1 {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn execute(_: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        UnaryExecutor::execute::<PhysicalF64, PhysicalF64, _>(
            &input.arrays()[0],
            input.selection(),
            OutBuffer::from_array(output)?,
            |v, buf| buf.put(&(v.exp_m1())),
        )
    }
}
