use glaredb_error::{DbError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_DEBUG_ERROR_ON_EXECUTE: ScalarFunctionSet = ScalarFunctionSet {
    name: "debug_error_on_execute",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Debug,
        description: "Return an error when this function gets executed.",
        arguments: &[],
        example: Some(Example {
            example: "error()",
            output: "ERROR: error function was executed",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[], DataTypeId::Int32),
        &DebugErrorOnExecute,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct DebugErrorOnExecute;

impl ScalarFunction for DebugErrorOnExecute {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int32(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, _input: &Batch, _output: &mut Array) -> Result<()> {
        Err(DbError::new("Debug error on execute"))
    }
}
