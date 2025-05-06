use glaredb_error::{Result, ResultExt};
use regex::{Captures, Regex, Replacer};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::{BinaryExecutor, TernaryExecutor, UnaryExecutor};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

pub const FUNCTION_SET_REGEXP_REPLACE: ScalarFunctionSet = ScalarFunctionSet {
    name: "regexp_replace",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Regexp,
        description: "Replace the first regular expression match in a string.",
        arguments: &["string", "regexp", "replacement"],
        example: Some(Example {
            example: "regexp_replace('alphabet', '[ae]', 'DOG')",
            output: "DOGlphabet",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
            DataTypeId::Utf8,
        ),
        &RegexpReplace,
    )],
};

#[derive(Debug)]
pub struct RegexpReplaceState {
    pattern: Option<Regex>,
    replacement: Option<PostgresReplacement<String>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RegexpReplace;

impl ScalarFunction for RegexpReplace {
    type State = RegexpReplaceState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let pattern = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;
            let pattern = Regex::new(&pattern).context("Failed to build regexp pattern")?;

            Some(pattern)
        } else {
            None
        };

        let replacement = if inputs[2].is_const_foldable() {
            let replacement = ConstFold::rewrite(inputs[2].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(PostgresReplacement(replacement))
        } else {
            None
        };

        Ok(BindState {
            state: RegexpReplaceState {
                pattern,
                replacement,
            },
            return_type: DataType::utf8(),
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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
                        let out = pattern.replace(s, &PostgresReplacement(replacement));
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

                        let out = pattern.replace(s, &PostgresReplacement(replacement));
                        buf.put(out.as_ref());
                    },
                )
            }
        }
    }
}

/// Replacement implementation that matches postgres semantics for string
/// replacement.
///
/// We walk the replacement string by hand such that:
//
/// - \1 => the text of capture group 1
/// - \2 => group 2, etc.
/// - \\ => a literal backslash.
/// - any other \x => literal x
#[derive(Debug)]
struct PostgresReplacement<S: AsRef<str> + ?Sized>(S);

impl<S> Replacer for &PostgresReplacement<S>
where
    S: AsRef<str>,
{
    fn replace_append(&mut self, caps: &Captures<'_>, dst: &mut String) {
        let mut chars = self.0.as_ref().chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.peek() {
                    // \ followed by digit => group capture
                    Some(d) if d.is_ascii_digit() => {
                        let idx = chars.next().unwrap().to_digit(10).unwrap() as usize;
                        if let Some(m) = caps.get(idx) {
                            dst.push_str(m.as_str());
                        }
                    }
                    // \\ => literal backslash
                    Some('\\') => {
                        chars.next();
                        dst.push('\\');
                    }
                    // \x where x not digit or backslash => literal x
                    Some(other) => {
                        dst.push(*other);
                        chars.next();
                    }
                    None => {
                        // trailing backslash => literal
                        dst.push('\\');
                    }
                }
            } else {
                dst.push(c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_replacement_tests() {
        // (haystack, pattern, replace, expected)
        let cases = [
            ("foobarbaz", "b..", "X", "fooXbaz"),
            ("alphabet", "[ae]", "DOG", "DOGlphabet"),
            ("foobarbaz", "b(..)", r#"X\1Y"#, "fooXarYbaz"), // Capture group
            ("foobarbaz", "b(..)", r#"X\1Y\1"#, "fooXarYarbaz"), // Capture group multiple times
            ("foobarbaz", "b(..)", r#"X\2Y"#, "fooXYbaz"),   // Capture group doesn't exist
            ("foobarbaz", "b(..).*a(.)", r#"\1X\2Y"#, "fooarXzY"), // Multiple capture groups
            ("foobarbaz", "b(..).*a(.)", r#"\\1X\2Y"#, r#"foo\1XzY"#), // Literal slash
        ];

        for case in cases {
            let regex = Regex::new(case.1).unwrap();
            let out = regex.replace(case.0, &PostgresReplacement(case.2));
            assert_eq!(case.3, out);
        }
    }
}
