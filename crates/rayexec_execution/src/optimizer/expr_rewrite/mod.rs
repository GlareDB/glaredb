pub mod const_fold;
pub mod distributive_or;
pub mod join_filter_or;
pub mod like;
pub mod unnest_conjunction;

use const_fold::ConstFold;
use distributive_or::DistributiveOrRewrite;
use join_filter_or::JoinFilterOrRewrite;
use like::LikeRewrite;
use rayexec_error::Result;
use unnest_conjunction::UnnestConjunctionRewrite;

use super::OptimizeRule;
use crate::expr::{self, Expression};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::{LogicalNode, LogicalOperator};

pub trait ExpressionRewriteRule {
    /// Rewrite a single expression.
    ///
    /// If the rewrite doesn't apply, then the expression should be returned
    /// unmodified.
    fn rewrite(expression: Expression) -> Result<Expression>;
}

/// Rewrites expression to be amenable to futher optimization.
#[derive(Debug)]
pub struct ExpressionRewriter;

impl OptimizeRule for ExpressionRewriter {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut plan = match plan {
            LogicalOperator::Project(mut project) => {
                project.node.projections = Self::apply_rewrites_all(project.node.projections)?;
                LogicalOperator::Project(project)
            }
            LogicalOperator::Filter(mut filter) => {
                filter.node.filter = Self::apply_rewrites(filter.node.filter)?;
                filter.node.filter = JoinFilterOrRewrite::rewrite(filter.node.filter)?; // Special rewrite for join filter condition.
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::ArbitraryJoin(mut join) => {
                join.node.condition = Self::apply_rewrites(join.node.condition)?;
                join.node.condition = JoinFilterOrRewrite::rewrite(join.node.condition)?; // Special rewrite for join filter condition.
                LogicalOperator::ArbitraryJoin(join)
            }
            mut other => {
                other.for_each_expr_mut(&mut |expr| {
                    // Replace with temp dummy value.
                    let mut orig = std::mem::replace(expr, expr::lit(83).into());
                    orig = Self::apply_rewrites(orig)?;
                    *expr = orig;
                    Ok(())
                })?;
                other
            }
        };

        plan.modify_replace_children(&mut |child| self.optimize(bind_context, child))?;

        Ok(plan)
    }
}

impl ExpressionRewriter {
    pub fn apply_rewrites_all(exprs: Vec<Expression>) -> Result<Vec<Expression>> {
        exprs
            .into_iter()
            .map(|expr| Self::apply_rewrites(expr))
            .collect::<Result<Vec<_>>>()
    }

    /// Apply all rewrite rules to an expression.
    pub fn apply_rewrites(expr: Expression) -> Result<Expression> {
        let expr = LikeRewrite::rewrite(expr)?; // TODO: Move to last
        let expr = ConstFold::rewrite(expr)?;
        let expr = UnnestConjunctionRewrite::rewrite(expr)?;
        let expr = DistributiveOrRewrite::rewrite(expr)?;
        // TODO: Undecided if we want to try to unnest again.
        Ok(expr)
    }
}
