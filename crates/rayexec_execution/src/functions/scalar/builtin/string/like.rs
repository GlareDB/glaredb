use rayexec_error::{Result, ResultExt};
use regex::{escape, Regex};

use crate::arrays::array::Array2;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer};
use crate::arrays::executor::physical_type::PhysicalUtf8_2;
use crate::arrays::executor::scalar::{BinaryExecutor2, UnaryExecutor2};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Like;

impl FunctionInfo for Like {
    fn name(&self) -> &'static str {
        "like"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // like(input, pattern)
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
                variadic_arg: None,
                return_type: DataTypeId::Boolean,
                doc: Some(&Documentation {
                    category: Category::String,
                    description: "Check if a string matches the given pattern.",
                    arguments: &["string", "pattern"],
                    example: Some(Example {
                        example: "like('hello, world', '%world')",
                        output: "true",
                    }),
                }),
            },
        ]
    }
}

impl ScalarFunction for Like {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Utf8, DataType::Utf8) => (),
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        }

        let function_impl: Box<dyn ScalarFunctionImpl> = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(table_list, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            let pattern = like_pattern_to_regex(&mut String::new(), &pattern, Some('\\'))?;

            Box::new(LikeConstImpl { constant: pattern })
        } else {
            Box::new(LikeImpl)
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LikeConstImpl {
    pub constant: Regex,
}

impl ScalarFunctionImpl for LikeConstImpl {
    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };

        UnaryExecutor2::execute::<PhysicalUtf8_2, _, _>(inputs[0], builder, |s, buf| {
            let b = self.constant.is_match(s);
            buf.put(&b);
        })
    }
}

#[derive(Debug, Clone)]
pub struct LikeImpl;

impl ScalarFunctionImpl for LikeImpl {
    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };

        let mut s_buf = String::new();

        BinaryExecutor2::execute::<PhysicalUtf8_2, PhysicalUtf8_2, _, _>(
            inputs[0],
            inputs[1],
            builder,
            |a, b, buf| {
                match like_pattern_to_regex(&mut s_buf, b, Some('\\')) {
                    Ok(pat) => {
                        let b = pat.is_match(a);
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
