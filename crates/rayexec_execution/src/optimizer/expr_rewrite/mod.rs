pub mod distributive_or;
pub mod join_filter_or;
pub mod unnest_conjunction;

use crate::{
    expr::Expression,
    logical::{
        binder::bind_context::BindContext, logical_join::ComparisonCondition,
        operator::LogicalOperator,
    },
};
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
    fn rewrite(expression: Expression) -> Result<Expression>;
}

/// Rewrites expression to be amenable to futher optimization.
#[derive(Debug)]
pub struct ExpressionRewriter;

impl OptimizeRule for ExpressionRewriter {
    fn optimize(
        &mut self,
        _bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut plan = match plan {
            LogicalOperator::Project(mut project) => {
                project.node.projections = Self::apply_rewrites_all(project.node.projections)?;
                LogicalOperator::Project(project)
            }
            LogicalOperator::Filter(mut filter) => {
                filter.node.filter = Self::apply_rewrites(filter.node.filter)?;
                filter.node.filter = JoinFilterOrRewrite::rewrite(filter.node.filter)?; // Special rewrite for join condition.
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::ArbitraryJoin(mut join) => {
                join.node.condition = Self::apply_rewrites(join.node.condition)?;
                join.node.condition = JoinFilterOrRewrite::rewrite(join.node.condition)?; // Special rewrite for join condition.
                LogicalOperator::ArbitraryJoin(join)
            }
            LogicalOperator::ComparisonJoin(mut join) => {
                join.node.conditions = join
                    .node
                    .conditions
                    .into_iter()
                    .map(|cond| {
                        Ok(ComparisonCondition {
                            left: Self::apply_rewrites(cond.left)?,
                            right: Self::apply_rewrites(cond.right)?,
                            op: cond.op,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                LogicalOperator::ComparisonJoin(join)
            }
            LogicalOperator::Aggregate(mut agg) => {
                agg.node.aggregates = Self::apply_rewrites_all(agg.node.aggregates)?;
                agg.node.group_exprs = Self::apply_rewrites_all(agg.node.group_exprs)?;
                LogicalOperator::Aggregate(agg)
            }
            other => other,
        };

        plan.modify_replace_children(&mut |child| self.optimize(_bind_context, child))?;

        Ok(plan)
    }
}

impl ExpressionRewriter {
    pub fn apply_rewrites_all(exprs: Vec<Expression>) -> Result<Vec<Expression>> {
        exprs
            .into_iter()
            .map(Self::apply_rewrites)
            .collect::<Result<Vec<_>>>()
    }

    /// Apply all rewrite rules to an expression.
    pub fn apply_rewrites(expr: Expression) -> Result<Expression> {
        let expr = UnnestConjunctionRewrite::rewrite(expr)?;
        let expr = DistributiveOrRewrite::rewrite(expr)?;
        // TODO: Undecided if we want to try to unnest again.
        Ok(expr)
    }
}
