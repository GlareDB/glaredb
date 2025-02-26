use rayexec_error::Result;

use crate::arrays::array::physical_type::{PhysicalBool, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor};
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

pub const FUNCTION_SET_STARTS_WITH: ScalarFunctionSet = ScalarFunctionSet {
    name: "starts_with",
    aliases: &["prefix"],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Check if a string starts with a prefix.",
        arguments: &["string", "prefix"],
        example: Some(Example {
            example: "starts_with('hello', 'he')",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Boolean),
        &StartsWith,
    )],
};

#[derive(Debug)]
pub struct StartsWithState {
    constant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartsWith;

impl ScalarFunction for StartsWith {
    type State = StartsWithState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let constant = if inputs[1].is_const_foldable() {
            let search_string = ConstFold::rewrite(inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(search_string)
        } else {
            None
        };

        Ok(BindState {
            state: StartsWithState { constant },
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match state.constant.as_ref() {
            Some(prefix) => UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _>(
                &input.arrays()[0],
                sel,
                OutBuffer::from_array(output)?,
                |s, buf| buf.put(&s.starts_with(prefix)),
            ),
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalBool, _>(
                &input.arrays()[0],
                sel,
                &input.arrays()[1],
                sel,
                OutBuffer::from_array(output)?,
                |s, prefix, buf| buf.put(&s.starts_with(prefix)),
            ),
        }
    }
}
