use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UnaryExecutor};
use rayexec_error::{Result, ResultExt};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::util_types;
use regex::{escape, Regex};

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction2, ScalarFunction};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
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
                input: &[DataTypeId::Utf8, DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
        ]
    }
}

impl ScalarFunction for Like {
    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        let constant: util_types::OptionalString = PackedDecoder::new(state).decode_next()?;
        let constant = constant
            .value
            .as_ref()
            .map(|s| Regex::new(s))
            .transpose()
            .context("Failed to rebuild regex")?;

        Ok(Box::new(LikeImpl { constant }))
    }

    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction2>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        match (&datatypes[0], &datatypes[1]) {
            (DataType::Utf8, DataType::Utf8) => (),
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        }

        let pattern = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(bind_context, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            let pattern = like_pattern_to_regex(&mut String::new(), &pattern, Some('\\'))?;

            Some(pattern)
        } else {
            None
        };

        Ok(Box::new(LikeImpl { constant: pattern }))
    }
}

#[derive(Debug, Clone)]
pub struct LikeImpl {
    pub constant: Option<Regex>,
}

impl PlannedScalarFunction2 for LikeImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Like
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let constant = self.constant.as_ref().map(|c| c.to_string());
        PackedEncoder::new(state).encode_next(&util_types::OptionalString { value: constant })
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };

        match self.constant.as_ref() {
            Some(constant) => {
                UnaryExecutor::execute::<PhysicalUtf8, _, _>(inputs[0], builder, |s, buf| {
                    let b = constant.is_match(s);
                    buf.put(&b);
                })
            }
            None => {
                let mut s_buf = String::new();

                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
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
