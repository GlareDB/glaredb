pub mod condition_extractor;
pub mod split;

use std::collections::HashSet;

use condition_extractor::{ExprJoinSide, JoinConditionExtractor};
use rayexec_error::{RayexecError, Result};
use split::split_conjunction;

use crate::{
    expr::Expression,
    logical::{
        binder::bind_context::{BindContext, TableRef},
        logical_filter::LogicalFilter,
        logical_join::{JoinType, LogicalArbitraryJoin, LogicalComparisonJoin, LogicalCrossJoin},
        logical_order::LogicalOrder,
        logical_project::LogicalProject,
        operator::{LocationRequirement, LogicalNode, LogicalOperator, Node},
        planner::plan_from::FromPlanner,
    },
};

use super::OptimizeRule;

/// Holds a filtering expression and all table refs the expression references.
#[derive(Debug)]
struct ExtractedFilter {
    /// The filter expression.
    filter: Expression,
    /// Tables refs this expression references.
    tables_refs: HashSet<TableRef>,
}

impl ExtractedFilter {
    fn from_expr(expr: Expression) -> Self {
        fn inner(child: &Expression, refs: &mut HashSet<TableRef>) {
            match child {
                Expression::Column(col) => {
                    refs.insert(col.table_scope);
                }
                other => other
                    .for_each_child(&mut |child| {
                        inner(child, refs);
                        Ok(())
                    })
                    .expect("getting table refs to not fail"),
            }
        }

        let mut refs = HashSet::new();
        inner(&expr, &mut refs);

        ExtractedFilter {
            filter: expr,
            tables_refs: refs,
        }
    }
}

#[derive(Debug, Default)]
pub struct FilterPushdownRule {
    filters: Vec<ExtractedFilter>,
}

impl OptimizeRule for FilterPushdownRule {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Filter(filter) => self.pushdown_filter(bind_context, filter),
            LogicalOperator::CrossJoin(join) => self.pushdown_cross_join(bind_context, join),
            LogicalOperator::ArbitraryJoin(join) => {
                self.pushdown_arbitrary_join(bind_context, join)
            }
            LogicalOperator::ComparisonJoin(join) => {
                self.pushdown_comparison_join(bind_context, join)
            }
            LogicalOperator::Project(project) => self.pushdown_project(bind_context, project),
            LogicalOperator::Order(order) => self.pushdown_order_by(bind_context, order),
            other => self.stop_pushdown(bind_context, other),
        }
    }
}

impl FilterPushdownRule {
    /// Adds an expression as a filter that we'll be pushing down.
    fn add_filter(&mut self, expr: Expression) {
        let mut split = Vec::new();
        split_conjunction(expr, &mut split);

        self.filters
            .extend(split.into_iter().map(ExtractedFilter::from_expr))
    }

    /// Stops the push down for this set of filters, and wraps the plan in a new
    /// filter node.
    ///
    /// This will go ahead and perform a separate pushdown to children of this
    /// plan.
    fn stop_pushdown(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Continue with a separate pushdown step for the children.
        let mut children = Vec::with_capacity(plan.children().len());
        for mut child in plan.children_mut().drain(..) {
            let mut pushdown = FilterPushdownRule::default();
            child = pushdown.optimize(bind_context, child)?;
            children.push(child)
        }
        *plan.children_mut() = children;

        if self.filters.is_empty() {
            // No remaining filters.
            return Ok(plan);
        }

        let filter = Expression::and_all(self.filters.drain(..).map(|ex| ex.filter))
            .expect("expression to be created from non-empty iter");

        Ok(LogicalOperator::Filter(Node {
            node: LogicalFilter { filter },
            location: LocationRequirement::Any,
            children: vec![plan],
        }))
    }

    fn pushdown_project(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalProject>,
    ) -> Result<LogicalOperator> {
        /// Replaces an expression that's referencing the project with the
        /// expression from the project by cloning it.
        fn replace_projection_reference(
            project: &LogicalProject,
            expr: &mut Expression,
        ) -> Result<()> {
            match expr {
                Expression::Column(col) => {
                    // Filters should only be referencing their child. If this
                    // filter expression isn't, then that's definitely a bug.
                    if col.table_scope != project.projection_table {
                        return Err(RayexecError::new(format!("Filter not referencing projection, filter ref: {col}, projection table: {}", project.projection_table)));
                    }
                    if col.column >= project.projections.len() {
                        return Err(RayexecError::new(format!(
                            "Filter referencing column outside of projection, filter ref: {col}"
                        )));
                    }

                    *expr = project.projections[col.column].clone();

                    Ok(())
                }
                other => other
                    .for_each_child_mut(&mut |child| replace_projection_reference(project, child)),
            }
        }

        let mut child_pushdown = Self::default();

        // Drain current filters to replace column references with concrete
        // expressions from the project.
        for filter in self.filters.drain(..) {
            let mut expr = filter.filter;
            replace_projection_reference(&plan.node, &mut expr)?;

            let filter = ExtractedFilter::from_expr(expr);
            child_pushdown.filters.push(filter);
        }

        let mut new_children = Vec::with_capacity(plan.children.len());
        for child in plan.children {
            let child = child_pushdown.optimize(bind_context, child)?;
            new_children.push(child);
        }

        plan.children = new_children;

        Ok(LogicalOperator::Project(plan))
    }

    fn pushdown_order_by(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalOrder>,
    ) -> Result<LogicalOperator> {
        let mut child = plan.take_one_child_exact()?;
        child = self.optimize(bind_context, child)?;
        plan.children = vec![child];

        Ok(LogicalOperator::Order(plan))
    }

    /// Pushes down through a filter node.
    ///
    /// This will extract the filter expressions from this node and append them
    /// to the rule's current filter list.
    fn pushdown_filter(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalFilter>,
    ) -> Result<LogicalOperator> {
        let child = plan.take_one_child_exact()?;
        self.add_filter(plan.node.filter);
        self.optimize(bind_context, child)
    }

    fn pushdown_arbitrary_join(
        &mut self,
        bind_context: &mut BindContext,
        plan: Node<LogicalArbitraryJoin>,
    ) -> Result<LogicalOperator> {
        match plan.node.join_type {
            JoinType::Inner => {
                // Convert to cross join, push down on cross join.
                self.add_filter(plan.node.condition);

                let plan = Node {
                    node: LogicalCrossJoin,
                    location: plan.location,
                    children: plan.children,
                };

                self.pushdown_cross_join(bind_context, plan)
            }
            // TODO: Other optimizations.
            _ => self.stop_pushdown(bind_context, LogicalOperator::ArbitraryJoin(plan)),
        }
    }

    fn pushdown_comparison_join(
        &mut self,
        bind_context: &mut BindContext,
        plan: Node<LogicalComparisonJoin>,
    ) -> Result<LogicalOperator> {
        match plan.node.join_type {
            JoinType::Inner => {
                // Convert to cross join, push down on cross join.
                for cond in plan.node.conditions {
                    let expr = cond.into_expression();
                    self.add_filter(expr);
                }

                let plan = Node {
                    node: LogicalCrossJoin,
                    location: plan.location,
                    children: plan.children,
                };

                self.pushdown_cross_join(bind_context, plan)
            }
            // TODO: Other optimizations.
            _ => self.stop_pushdown(bind_context, LogicalOperator::ComparisonJoin(plan)),
        }
    }

    /// Push down through a cross join.
    fn pushdown_cross_join(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalCrossJoin>,
    ) -> Result<LogicalOperator> {
        if self.filters.is_empty() {
            // Nothing to possible join on.
            return Ok(LogicalOperator::CrossJoin(plan));
        }

        let mut left_pushdown = Self::default();
        let mut right_pushdown = Self::default();

        let [mut left, mut right] = plan.take_two_children_exact()?;

        let left_tables = left.get_output_table_refs();
        let right_tables = right.get_output_table_refs();

        let mut join_exprs = Vec::new();

        // Figure out which expressions we can push further down vs which are
        // part of the join expression.
        for filter in self.filters.drain(..) {
            let side = ExprJoinSide::try_from_table_refs(
                &filter.tables_refs,
                &left_tables,
                &right_tables,
            )?;

            match side {
                ExprJoinSide::Left => {
                    // Filter only depends on left input.
                    left_pushdown.filters.push(filter);
                }
                ExprJoinSide::Right => {
                    // Filter only depends on right input.
                    right_pushdown.filters.push(filter);
                }
                ExprJoinSide::Both | ExprJoinSide::None => {
                    // Filter is join condition.
                    join_exprs.push(filter.filter);
                }
            }
        }

        // Do the left/right pushdowns first.
        left = left_pushdown.optimize(bind_context, left)?;
        right = right_pushdown.optimize(bind_context, right)?;

        if join_exprs.is_empty() {
            // We've pushed filters to left/right operators, but have none
            // remaining for this node.
            return Ok(LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![left, right],
            }));
        }

        // Extract join conditions.
        let extractor = JoinConditionExtractor::new(&left_tables, &right_tables, JoinType::Inner);
        let conditions = extractor.extract(join_exprs)?;

        // We're attempting to do an INNER join and we've already pulled out
        // filters that get pushed to the left/right ops. Both of these should
        // be empty.
        if !conditions.left_filter.is_empty() {
            return Err(RayexecError::new(
                "Left filters unexpectedly has expression",
            ));
        }
        if !conditions.right_filter.is_empty() {
            return Err(RayexecError::new(
                "Right filters unexpectedly has expression",
            ));
        }

        // Create the join using the extracted conditions.
        FromPlanner.plan_join_from_conditions(
            JoinType::Inner,
            conditions.comparisons,
            conditions.arbitrary,
            left,
            right,
        )
    }
}
