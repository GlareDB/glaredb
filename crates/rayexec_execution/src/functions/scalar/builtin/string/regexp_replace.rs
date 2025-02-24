use rayexec_error::{Result, ResultExt};
use regex::Regex;

use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryExecutor, TernaryExecutor, UnaryExecutor};
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

pub const FUNCTION_SET_REGEXP_REPLACE: ScalarFunctionSet = ScalarFunctionSet {
    name: "regexp_replace",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Regexp,
        description: "Replace the first regular expression match in a string.",
        arguments: &["string", "regexp", "replacement"],
        example: Some(Example {
            example: "regexp_replace('alphabet', '[ae]', 'DOG')",
            output: "DOGlphabet",
        }),
    }),
    functions: &[RawScalarFunction::new(
        Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
            DataTypeId::Utf8,
        ),
        &RegexpReplace,
    )],
};

#[derive(Debug)]
pub struct RegexpReplaceState {
    pattern: Option<Regex>,
    replacement: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RegexpReplace;

impl ScalarFunction for RegexpReplace {
    type State = RegexpReplaceState;

    fn bind(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<BindState<Self::State>> {
        let pattern = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(table_list, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;
            let pattern = Regex::new(&pattern).context("Failed to build regexp pattern")?;

            Some(pattern)
        } else {
            None
        };

        let replacement = if inputs[2].is_const_foldable() {
            let replacement = ConstFold::rewrite(table_list, inputs[2].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(replacement)
        } else {
            None
        };

        Ok(BindState {
            state: RegexpReplaceState {
                pattern,
                replacement,
            },
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn execute(&self, state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match (state.pattern.as_ref(), state.replacement.as_ref()) {
            (Some(pattern), Some(replacement)) => {
                UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
                    &input.arrays()[0],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, buf| {
                        // TODO: Flags to more many.
                        let out = pattern.replace(s, replacement);
                        buf.put(out.as_ref());
                    },
                )
            }
            (Some(pattern), None) => {
                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _>(
                    &input.arrays()[0],
                    sel,
                    &input.arrays()[2],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, replacement, buf| {
                        let out = pattern.replace(s, replacement);
                        buf.put(out.as_ref());
                    },
                )
            }
            (None, Some(replacement)) => {
                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _>(
                    &input.arrays()[0],
                    sel,
                    &input.arrays()[1],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, pattern, buf| {
                        let pattern = match Regex::new(pattern) {
                            Ok(pattern) => pattern,
                            Err(_) => {
                                // TODO: Do something.
                                return;
                            }
                        };

                        let out = pattern.replace(s, replacement);
                        buf.put(out.as_ref());
                    },
                )
            }
            (None, None) => {
                TernaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _>(
                    &input.arrays()[0],
                    sel,
                    &input.arrays()[1],
                    sel,
                    &input.arrays()[2],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, pattern, replacement, buf| {
                        let pattern = match Regex::new(pattern) {
                            Ok(pattern) => pattern,
                            Err(_) => {
                                // TODO: Do something.
                                return;
                            }
                        };

                        let out = pattern.replace(s, replacement);
                        buf.put(out.as_ref());
                    },
                )
            }
        }
    }
}
