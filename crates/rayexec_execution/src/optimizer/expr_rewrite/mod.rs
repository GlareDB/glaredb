pub mod const_fold;
pub mod distributive_or;
pub mod join_filter_or;
pub mod unnest_conjunction;

use crate::{
    expr::{self, Expression},
    logical::{
        binder::bind_context::BindContext,
        operator::{LogicalNode, LogicalOperator},
    },
};
use const_fold::ConstFold;
use distributive_or::DistributiveOrRewrite;
use join_filter_or::JoinFilterOrRewrite;
use rayexec_error::Result;
use unnest_conjunction::UnnestConjunctionRewrite;

use super::OptimizeRule;

pub trait ExpressionRewriteRule {
    /// Rewrite a single expression.
    ///
    /// If the rewrite doesn't apply, then the expression should be returned
    /// unmodified.
    fn rewrite(bind_context: &BindContext, expression: Expression) -> Result<Expression>;
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
            LogicalOperator::Filter(mut filter) => {
                filter.node.filter = Self::apply_rewrites(bind_context, filter.node.filter)?;
                filter.node.filter =
                    JoinFilterOrRewrite::rewrite(bind_context, filter.node.filter)?; // Special rewrite for join filter condition.
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::ArbitraryJoin(mut join) => {
                join.node.condition = Self::apply_rewrites(bind_context, join.node.condition)?;
                join.node.condition =
                    JoinFilterOrRewrite::rewrite(bind_context, join.node.condition)?; // Special rewrite for join filter condition.
                LogicalOperator::ArbitraryJoin(join)
            }
            mut other => {
                other.for_each_expr_mut(&mut |expr| {
                    let mut orig = std::mem::replace(expr, expr::lit(83));
                    orig = Self::apply_rewrites(bind_context, orig)?;
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
    pub fn apply_rewrites_all(
        bind_context: &BindContext,
        exprs: Vec<Expression>,
    ) -> Result<Vec<Expression>> {
        exprs
            .into_iter()
            .map(|expr| Self::apply_rewrites(bind_context, expr))
            .collect::<Result<Vec<_>>>()
    }

    /// Apply all rewrite rules to an expression.
    pub fn apply_rewrites(bind_context: &BindContext, expr: Expression) -> Result<Expression> {
        let expr = ConstFold::rewrite(bind_context, expr)?;
        let expr = UnnestConjunctionRewrite::rewrite(bind_context, expr)?;
        let expr = DistributiveOrRewrite::rewrite(bind_context, expr)?;
        // TODO: Undecided if we want to try to unnest again.
        Ok(expr)
    }
}
