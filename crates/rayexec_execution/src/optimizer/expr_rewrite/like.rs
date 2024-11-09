use rayexec_error::Result;

use super::ExpressionRewriteRule;
use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::scalar_function_expr::ScalarFunctionExpr;
use crate::expr::{self, Expression};
use crate::functions::scalar::like::Like;
use crate::functions::scalar::string::{EndsWithImpl, StartsWithImpl, StringContainsImpl};
use crate::functions::FunctionInfo;
use crate::logical::binder::bind_context::BindContext;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

/// Rewrite LIKE expressions into equivalent prefix/suffix/contains calls if
/// possible.
#[derive(Debug)]
pub struct LikeRewrite;

impl ExpressionRewriteRule for LikeRewrite {
    fn rewrite(bind_context: &BindContext, mut expression: Expression) -> Result<Expression> {
        fn inner(bind_context: &BindContext, expr: &mut Expression) -> Result<()> {
            match expr {
                Expression::ScalarFunction(scalar)
                    if scalar.function.scalar_function().name() == Like.name() =>
                {
                    let pattern = &scalar.inputs[1];
                    if !pattern.is_const_foldable() {
                        return Ok(());
                    }

                    let pattern = ConstFold::rewrite(bind_context, pattern.clone())?
                        .try_into_scalar()?
                        .try_into_string()?;

                    if can_str_compare(&pattern) {
                        *expr = Expression::Comparison(ComparisonExpr {
                            left: Box::new(scalar.inputs[0].clone()),
                            right: Box::new(expr::lit(pattern)),
                            op: ComparisonOperator::Eq,
                        });

                        Ok(())
                    } else if is_prefix_pattern(&pattern) {
                        let pattern = pattern.trim_matches('%').to_string();
                        *expr = Expression::ScalarFunction(ScalarFunctionExpr {
                            function: Box::new(StartsWithImpl {
                                constant: Some(pattern.clone()),
                            }),
                            inputs: vec![scalar.inputs[0].clone(), expr::lit(pattern)],
                        });

                        Ok(())
                    } else if is_suffix_pattern(&pattern) {
                        let pattern = pattern.trim_matches('%').to_string();
                        *expr = Expression::ScalarFunction(ScalarFunctionExpr {
                            function: Box::new(EndsWithImpl {
                                constant: Some(pattern.clone()),
                            }),
                            inputs: vec![scalar.inputs[0].clone(), expr::lit(pattern)],
                        });

                        Ok(())
                    } else if is_contains_pattern(&pattern) {
                        let pattern = pattern.trim_matches('%').to_string();
                        *expr = Expression::ScalarFunction(ScalarFunctionExpr {
                            function: Box::new(StringContainsImpl {
                                constant: Some(pattern.clone()),
                            }),
                            inputs: vec![scalar.inputs[0].clone(), expr::lit(pattern)],
                        });

                        Ok(())
                    } else {
                        // Leave unchanged.
                        Ok(())
                    }
                }
                other => other.for_each_child_mut(&mut |child| inner(bind_context, child)),
            }
        }

        inner(bind_context, &mut expression)?;

        Ok(expression)
    }
}

/// Checks if the string actually contains any pattern characters. If it
/// doesn't, we can just compare the strings direclty.
fn can_str_compare(s: &str) -> bool {
    !s.contains('%') && !s.contains('_')
}

fn is_contains_pattern(s: &str) -> bool {
    if s.len() < 2 {
        return false;
    }

    if s.as_bytes()[0] != b'%' {
        return false;
    }

    if s.as_bytes()[s.len() - 1] != b'%' {
        return false;
    }

    let sub = &s[1..s.len() - 1];

    // Check if trailing '%' was escaped.
    if !sub.is_empty() && sub.as_bytes()[sub.len() - 1] == b'\\' {
        return false;
    }

    if sub.contains('%') || sub.contains('_') {
        return false;
    }

    true
}

fn is_prefix_pattern(s: &str) -> bool {
    let pat_pos = match s.find('%') {
        Some(idx) => idx,
        None => return false,
    };

    if s.contains('_') {
        return false;
    }

    if pat_pos != s.len() - 1 {
        return false;
    }

    // Ensure '%' isn't escaped.
    if pat_pos != 0 && s.as_bytes()[pat_pos - 1] == b'\\' {
        return false;
    }

    true
}

fn is_suffix_pattern(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    if s.as_bytes()[0] != b'%' {
        return false;
    }

    if s[1..].contains('%') || s.contains('_') {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_compare() {
        let cases = [
            ("hello", true),
            ("%hello", false),
            ("%hello%", false),
            ("hello%", false),
            (r#"hello\%"#, false),
            ("he_llo%", false),
            ("he_llo", false),
            ("", true),
        ];

        for case in cases {
            let got = can_str_compare(case.0);
            assert_eq!(case.1, got, "{}", case.0);
        }
    }

    #[test]
    fn is_prefix() {
        let cases = [
            ("hello", false),
            ("%hello", false),
            ("%hello%", false),
            ("hello%", true),
            (r#"hello\%"#, false),
            ("he_llo%", false),
            ("", false),
        ];

        for case in cases {
            let got = is_prefix_pattern(case.0);
            assert_eq!(case.1, got, "{}", case.0);
        }
    }

    #[test]
    fn is_suffix() {
        let cases = [
            ("hello", false),
            ("%hello", true),
            (r#"\%hello"#, false),
            ("%hello%", false),
            ("hello%", false),
            ("%he_llo", false),
            ("", false),
        ];

        for case in cases {
            let got = is_suffix_pattern(case.0);
            assert_eq!(case.1, got, "{}", case.0);
        }
    }

    #[test]
    fn is_contains() {
        let cases = [
            ("hello", false),
            ("%hello", false),
            (r#"%hello\%"#, false),
            ("%hello%", true),
            ("hello%", false),
            ("%he_llo", false),
            ("", false),
            ("%%", true),
            ("%he_llo%", false),
        ];

        for case in cases {
            let got = is_contains_pattern(case.0);
            assert_eq!(case.1, got, "{}", case.0);
        }
    }
}
