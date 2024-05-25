use crate::{
    expr::scalar::BinaryOperator,
    planner::operator::{EqualityJoin, LogicalExpression, LogicalOperator},
};
use rayexec_error::Result;

use super::{walk_plan_post, OptimizeRule};

#[derive(Debug, Clone)]
pub struct JoinOrderRule {}

impl OptimizeRule for JoinOrderRule {
    fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.optimize_any_join_to_equality_join(plan)
    }
}

impl JoinOrderRule {
    /// Try to swap out any joins (joins with arbitrary expressions) with
    /// equality joins.
    fn optimize_any_join_to_equality_join(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        let plan = walk_plan_post(plan, &mut |plan| {
            match plan {
                LogicalOperator::AnyJoin(join) => {
                    let mut conjunctives = Vec::with_capacity(1);
                    split_conjuctive(join.on.clone(), &mut conjunctives);

                    // Used to adjust the indexes used for the on keys.
                    let left_len = join.left.output_schema(&[])?.types.len();

                    let mut left_on = Vec::new();
                    let mut right_on = Vec::new();

                    let mut remaining = Vec::new();
                    for expr in conjunctives {
                        // Currently this just does a basic 'col1 = col2' check.
                        // More sophisticated exprs can be represented to adding an
                        // additional projection to the input.
                        if let LogicalExpression::Binary {
                            op: BinaryOperator::Eq,
                            left,
                            right,
                        } = &expr
                        {
                            if let (
                                LogicalExpression::ColumnRef(left),
                                LogicalExpression::ColumnRef(right),
                            ) = (left.as_ref(), right.as_ref())
                            {
                                if let (Ok(left), Ok(right)) =
                                    (left.try_as_uncorrelated(), right.try_as_uncorrelated())
                                {
                                    // If correlated, then this would be a
                                    // lateral join. Unsure how we want to
                                    // optimize that right now.

                                    // Normal 'left_table_col = right_table_col'
                                    if left < left_len && right >= left_len {
                                        left_on.push(left);
                                        right_on.push(right - left_len);
                                        // This expression was handled, avoid
                                        // putting it in remaining.
                                        continue;
                                    }

                                    // May be flipped like 'right_table_col = left_table_col'
                                    if right < left_len && left >= left_len {
                                        left_on.push(right);
                                        right_on.push(left - left_len);
                                        // This expression was handled, avoid
                                        // putting it in remaining.
                                        continue;
                                    }
                                }
                            }
                        }

                        // Didn't handle this expression. Add it to remaining.
                        remaining.push(expr);
                    }

                    // We were able to extract all equalities. Update the plan
                    // to be an equality join.
                    if remaining.is_empty() {
                        *plan = LogicalOperator::EqualityJoin(EqualityJoin {
                            left: std::mem::replace(
                                &mut join.left,
                                Box::new(LogicalOperator::Empty),
                            ),
                            right: std::mem::replace(
                                &mut join.right,
                                Box::new(LogicalOperator::Empty),
                            ),
                            join_type: join.join_type,
                            left_on,
                            right_on,
                        });
                    }

                    Ok(())
                }
                _ => Ok(()), // Not a plan we can optimize.
            }
        })?;

        Ok(plan)
    }
}

/// Split a logical expression on AND conditions, appending them to the provided
/// vector.
fn split_conjuctive(expr: LogicalExpression, outputs: &mut Vec<LogicalExpression>) {
    match expr {
        LogicalExpression::Binary {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            split_conjuctive(*left, outputs);
            split_conjuctive(*right, outputs);
        }
        other => outputs.push(other),
    }
}
