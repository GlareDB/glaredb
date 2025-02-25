use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::Expression;

/// Generates additional filters based on input expressions.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FilterGenerator {
    /// Arbitrary expressions that we don't have a good heuristic for.
    arbitrary: Vec<Expression>,
    /// Sets of expressions that are equal to each other.
    equivalences: Vec<EquivalentSet>,
}

impl FilterGenerator {
    /// Adds an expression to the current set of filter.
    pub fn add_expression(&mut self, expr: Expression) {
        match expr {
            Expression::Comparison(cmp) if cmp.op == ComparisonOperator::Eq => {
                let mut exprs = (*cmp.left, *cmp.right);

                for set in &mut self.equivalences {
                    match set.try_insert(exprs) {
                        Ok(_) => return, // We're done.
                        Err(out) => exprs = out,
                    }
                }

                // If we get here, no existing equivalances matched, create a
                // new one.
                self.equivalences.push(EquivalentSet {
                    expressions: [exprs.0, exprs.1].into_iter().collect(),
                });
            }
            other => self.arbitrary.push(other),
        }
    }

    pub fn into_expressions(self) -> Vec<Expression> {
        let mut out = self.arbitrary;
        for mut set in self.equivalences {
            set.drain_into(&mut out);
        }

        out
    }

    pub fn is_empty(&self) -> bool {
        for set in &self.equivalences {
            if !set.is_empty() {
                return false;
            }
        }

        self.arbitrary.is_empty()
    }
}

/// A set of expressions that are all considered to be equal.
///
/// This lets us derive additional filters, e.g. inserting `a = b` and `b = c`
/// lets us derive `a = c`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EquivalentSet {
    expressions: Vec<Expression>,
}

impl EquivalentSet {
    fn is_empty(&self) -> bool {
        self.expressions.is_empty()
    }

    /// Try to insert the left and right expression of an equal comparison
    /// expression into the set.
    ///
    /// Returns the expressions unchanged if it cannot be inserted.
    fn try_insert(
        &mut self,
        (left, right): (Expression, Expression),
    ) -> Result<(), (Expression, Expression)> {
        let left_contains = self.expressions.contains(&left);
        let right_contains = self.expressions.contains(&right);

        if left_contains && right_contains {
            // Nothing to do, we already have both sides.
            return Ok(());
        }

        if left_contains {
            // Insert right.
            self.expressions.push(right);
            return Ok(());
        }

        if right_contains {
            // Insert left.
            self.expressions.push(left);
            return Ok(());
        }

        Err((left, right))
    }

    /// Drain all expressions into `out`, generated equality comparisons for
    /// each possible pair of expressions.
    fn drain_into(&mut self, out: &mut Vec<Expression>) {
        while let Some(expr) = self.expressions.pop() {
            for remaining in &self.expressions {
                let generated = Expression::Comparison(ComparisonExpr {
                    left: Box::new(remaining.clone()),
                    right: Box::new(expr.clone()),
                    op: ComparisonOperator::Eq,
                });

                out.push(generated)
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::expr;

//     #[test]
//     fn gen_equal_simple() {
//         let input1 = expr::eq(expr::col_ref(0, 0), expr::col_ref(1, 0));
//         let input2 = expr::eq(expr::col_ref(1, 0), expr::col_ref(2, 0));

//         let mut gen = FilterGenerator::default();
//         gen.add_expression(input1);
//         gen.add_expression(input2);

//         let out = gen.into_expressions();

//         // Order assumes knowledge of internals.
//         let expected = vec![
//             expr::eq(expr::col_ref(0, 0), expr::col_ref(2, 0)),
//             expr::eq(expr::col_ref(1, 0), expr::col_ref(2, 0)),
//             expr::eq(expr::col_ref(0, 0), expr::col_ref(1, 0)),
//         ];

//         assert_eq!(expected, out);
//     }

//     #[test]
//     fn gen_not_equal_simple() {
//         // TODO: We _could_ generated '(0,0) < (2,0)'
//         let input1 = expr::lt(expr::col_ref(0, 0), expr::col_ref(1, 0));
//         let input2 = expr::lt(expr::col_ref(1, 0), expr::col_ref(2, 0));

//         let mut gen = FilterGenerator::default();
//         gen.add_expression(input1);
//         gen.add_expression(input2);

//         let out = gen.into_expressions();

//         let expected = vec![
//             expr::lt(expr::col_ref(0, 0), expr::col_ref(1, 0)),
//             expr::lt(expr::col_ref(1, 0), expr::col_ref(2, 0)),
//         ];

//         assert_eq!(expected, out);
//     }
// }
