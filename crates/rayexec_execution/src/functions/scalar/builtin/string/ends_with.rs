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
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

pub const FUNCTION_SET_ENDS_WITH: ScalarFunctionSet = ScalarFunctionSet {
    name: "ends_with",
    aliases: &["suffix"],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Check if a string ends with a suffix.",
        arguments: &["string", "prefix"],
        example: Some(Example {
            example: "ends_with('house', 'se')",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Boolean),
        &EndsWith,
    )],
};

#[derive(Debug)]
pub struct EndsWithState {
    constant: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct EndsWith;

impl ScalarFunction for EndsWith {
    type State = EndsWithState;

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
            state: EndsWithState { constant },
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let strings = &input.arrays()[0];
        let suffix = &input.arrays()[1];

        match state.constant.as_ref() {
            Some(constant) => UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _>(
                strings,
                sel,
                OutBuffer::from_array(output)?,
                |s, buf| {
                    let v = s.ends_with(constant);
                    buf.put(&v);
                },
            ),
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalBool, _>(
                strings,
                sel,
                suffix,
                sel,
                OutBuffer::from_array(output)?,
                |s, suffix, buf| {
                    let v = s.ends_with(&suffix);
                    buf.put(&v);
                },
            ),
        }
    }
}
