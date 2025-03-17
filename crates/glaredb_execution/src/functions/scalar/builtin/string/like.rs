use glaredb_error::{Result, ResultExt};
use regex::{Regex, escape};

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

pub const FUNCTION_SET_LIKE: ScalarFunctionSet = ScalarFunctionSet {
    name: "like",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Check if a string matches the given pattern.",
        arguments: &["string", "pattern"],
        example: Some(Example {
            example: "like('hello, world', '%world')",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Boolean),
        &Like,
    )],
};

#[derive(Debug)]
pub struct LikeState {
    constant: Option<Regex>,
}

#[derive(Debug, Clone, Copy)]
pub struct Like;

impl ScalarFunction for Like {
    type State = LikeState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let constant = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            let pattern = like_pattern_to_regex(&mut String::new(), &pattern, Some('\\'))?;

            Some(pattern)
        } else {
            None
        };

        Ok(BindState {
            state: LikeState { constant },
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let strings = &input.arrays()[0];
        let patterns = &input.arrays()[1];

        match state.constant.as_ref() {
            Some(regex) => UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _>(
                strings,
                sel,
                OutBuffer::from_array(output)?,
                |s, buf| {
                    let b = regex.is_match(s);
                    buf.put(&b);
                },
            ),
            None => {
                let mut s_buf = String::new();
                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalBool, _>(
                    strings,
                    sel,
                    patterns,
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, pattern, buf| {
                        match like_pattern_to_regex(&mut s_buf, pattern, Some('\\')) {
                            Ok(pat) => {
                                let b = pat.is_match(s);
                                buf.put(&b);
                            }
                            Err(_) => {
                                // TODO: Do something
                            }
                        }
                    },
                )
            }
        }
    }
}

/// Converts a LIKE pattern into regex.
fn like_pattern_to_regex(
    buf: &mut String,
    pattern: &str,
    escape_char: Option<char>,
) -> Result<Regex> {
    buf.clear();
    buf.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if Some(c) == escape_char {
            // Escape character found, treat the next character literally.
            if let Some(next_char) = chars.next() {
                buf.push_str(&escape(&next_char.to_string()));
            } else {
                // Escape character at the end, treat it literally.
                buf.push_str(&escape(&c.to_string()));
            }
        } else {
            match c {
                '%' => {
                    buf.push_str(".*"); // '%' matches any sequence of characters
                }
                '_' => {
                    buf.push('.'); // '_' matches any single character
                }
                _ => {
                    // Escape regex special characters.
                    buf.push_str(&escape(&c.to_string()));
                }
            }
        }
    }
    buf.push('$');

    Regex::new(buf).context("Failed to build regex pattern")
}
