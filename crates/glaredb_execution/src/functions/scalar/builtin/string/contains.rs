use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalBool, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

pub const FUNCTION_SET_CONTAINS: ScalarFunctionSet = ScalarFunctionSet {
    name: "contains",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Check if string contains a search string.",
        arguments: &["string", "search"],
        example: Some(Example {
            example: "contains('house', 'ou')",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Boolean),
        &StringContains,
    )],
};

#[derive(Debug)]
pub struct StringContainsState {
    pub constant: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct StringContains;

impl ScalarFunction for StringContains {
    type State = StringContainsState;

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
            state: StringContainsState { constant },
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let haystack = &input.arrays()[0];
        let needle = &input.arrays()[1];

        match state.constant.as_ref() {
            Some(constant) => UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _>(
                haystack,
                sel,
                OutBuffer::from_array(output)?,
                |haystack, buf| {
                    let v = haystack.contains(constant);
                    buf.put(&v);
                },
            ),
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalBool, _>(
                haystack,
                sel,
                needle,
                sel,
                OutBuffer::from_array(output)?,
                |haystack, needle, buf| {
                    let v = haystack.contains(needle);
                    buf.put(&v);
                },
            ),
        }
    }
}
