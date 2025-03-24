pub mod condition_extractor;
pub mod extracted_filter;
pub mod generator;
pub mod split;

use condition_extractor::{ExprJoinSide, JoinConditionExtractor};
use extracted_filter::ExtractedFilter;
use generator::FilterGenerator;
use glaredb_error::{DbError, Result};
use split::split_conjunction;

use super::OptimizeRule;
use super::expr_rewrite::ExpressionRewriteRule;
use super::expr_rewrite::const_fold::ConstFold;
use crate::expr::{self, Expression};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableRef;
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::logical_distinct::LogicalDistinct;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_join::{
    JoinType,
    LogicalArbitraryJoin,
    LogicalComparisonJoin,
    LogicalCrossJoin,
    LogicalMagicJoin,
};
use crate::logical::logical_materialization::LogicalMaterializationScan;
use crate::logical::logical_no_rows::LogicalNoRows;
use crate::logical::logical_order::LogicalOrder;
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::planner::plan_from::FromPlanner;
use crate::logical::statistics::StatisticsValue;

// TODO: ExtractedFilter seems to not be entirely worth it here. There's
// frequent convert to/from it.

#[derive(Debug, Default)]
pub struct FilterPushdown {
    filter_gen: FilterGenerator,
}

impl OptimizeRule for FilterPushdown {
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
            LogicalOperator::MagicJoin(join) => self.pushdown_magic_join(bind_context, join),
            LogicalOperator::Project(project) => self.pushdown_project(bind_context, project),
            LogicalOperator::Aggregate(agg) => self.pushdown_aggregate(bind_context, agg),
            LogicalOperator::Order(order) => self.pushdown_order_by(bind_context, order),
            LogicalOperator::Distinct(distinct) => self.pushdown_distinct(bind_context, distinct),
            LogicalOperator::MaterializationScan(mat) => {
                self.pushdown_materialized_scan(bind_context, mat)
            }
            other => self.stop_pushdown(bind_context, other),
        }
    }
}

impl FilterPushdown {
    /// Adds expressions as a filter that we'll be pushing down.
    fn add_filters(&mut self, exprs: impl IntoIterator<Item = Expression>) {
        let mut split = Vec::new();
        for expr in exprs {
            split_conjunction(expr, &mut split);
        }

        for expr in split {
            self.filter_gen.add_expression(expr);
        }
    }

    fn drain_filters(&mut self) -> impl Iterator<Item = ExtractedFilter> + use<> {
        let f_gen = std::mem::take(&mut self.filter_gen);
        f_gen
            .into_expressions()
            .into_iter()
            .map(ExtractedFilter::from_expr)
    }

    /// Stops the push down for this set of filters, and possibly wraps the plan
    /// in a new filter node.
    ///
    /// If there are no remaining filters, then no additional fiter is added.
    ///
    /// If the remaining filter expression is constant, it's evaluated. If the
    /// result is true, then no filter node is added. If false, then we return
    /// an empty operator, effectively removing all children of the filter.
    ///
    /// This will go ahead and perform a separate pushdown to children of this
    /// plan.
    fn stop_pushdown(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Continue with a separate pushdown step for the children.
        plan.modify_replace_children(&mut |child| {
            let mut pushdown = FilterPushdown::default();
            pushdown.optimize(bind_context, child)
        })?;

        if self.filter_gen.is_empty() {
            // No remaining filters.
            return Ok(plan);
        }

        let filter: Expression = expr::and(self.drain_filters().map(|ex| ex.filter))?.into();

        if filter.is_const_foldable() {
            let val = ConstFold::rewrite(filter)?
                .try_into_scalar()?
                .try_as_bool()?;

            if val {
                // Omit the filter node, all rows pass.
                Ok(plan)
            } else {
                // Replace with no rows, reusing the same table refs as the
                // original plan.
                let table_refs = plan.get_output_table_refs(bind_context);

                Ok(LogicalOperator::NoRows(Node {
                    node: LogicalNoRows { table_refs },
                    location: LocationRequirement::Any,
                    children: Vec::new(),
                    estimated_cardinality: StatisticsValue::Exact(0),
                }))
            }
        } else {
            // TODO: We could probably const fold here to simplify as much as
            // possible as well.
            Ok(LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            }))
        }
    }

    fn pushdown_materialized_scan(
        &mut self,
        bind_context: &mut BindContext,
        plan: Node<LogicalMaterializationScan>,
    ) -> Result<LogicalOperator> {
        // TODO: I have no idea how this will work with recursively called
        // scans.

        // Note we may end up trying to optimize the materialized plan multiple
        // times if it's being scanned multiple times, but that shouldn't impact
        // anything, we'll just try to optimize an optimized plan some
        // additional number of times.
        let orig = {
            let mat = &mut bind_context.get_materialization_mut(plan.node.mat)?;
            std::mem::replace(&mut mat.plan, LogicalOperator::Invalid)
        };

        // Optimize the materialized plan _without_ our current set of
        // filters.
        let mut pushdown = FilterPushdown::default();
        let optimized = pushdown.optimize(bind_context, orig)?;

        let mat = bind_context.get_materialization_mut(plan.node.mat)?;
        mat.plan = optimized;

        // Ensure we wrap this scan in any remaining filters.
        self.stop_pushdown(bind_context, LogicalOperator::MaterializationScan(plan))
    }

    /// Push down through a project.
    ///
    /// Column references for stored filters will be updated to point to the
    /// children of the projection and not the projection itself.
    fn pushdown_project(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalProject>,
    ) -> Result<LogicalOperator> {
        let mut child_pushdown = Self::default();

        // Drain current filters to replace column references with concrete
        // expressions from the project.
        for filter in self.drain_filters() {
            let mut expr = filter.filter;
            replace_references(
                &plan.node.projections,
                plan.node.projection_table,
                &mut expr,
            )?;

            child_pushdown.add_filters([expr]);
        }

        let mut new_children = Vec::with_capacity(plan.children.len());
        for child in plan.children {
            let child = child_pushdown.optimize(bind_context, child)?;
            new_children.push(child);
        }

        plan.children = new_children;

        Ok(LogicalOperator::Project(plan))
    }

    fn pushdown_aggregate(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalAggregate>,
    ) -> Result<LogicalOperator> {
        let mut child_pushdown = Self::default();

        let mut remaining_filters = Vec::new();

        // Find filters that we can pushdown, replacing column references as
        // needed.
        //
        // This can only push down filters that refernence columns in the group
        // by.
        for filter in self.drain_filters() {
            // Cannot pushdown filter referencing aggregate output.
            if filter.table_refs.contains(&plan.node.aggregates_table) {
                remaining_filters.push(filter);
                continue;
            }

            let grouping_sets = match &plan.node.grouping_sets {
                Some(sets) => sets,
                None => {
                    // No GROUP BY, nothing to match on.
                    remaining_filters.push(filter);
                    continue;
                }
            };

            let group_table = plan
                .node
                .group_table
                .expect("group table ref must exist if grouping sets exists");

            let expr_cols = filter.filter.get_column_references();

            // Check that all grouping sets contain all column references in the
            // filter.
            let all_contain = grouping_sets.iter().all(|grouping_set| {
                expr_cols.iter().all(|expr_col| {
                    // Expr can only reference the grouping table or the agg
                    // outputs. We already check the agg outputs above, so this
                    // must be true.
                    debug_assert_eq!(group_table, expr_col.table_scope);

                    grouping_set.contains(&expr_col.column)
                })
            });

            if !all_contain {
                remaining_filters.push(filter);
                continue;
            }

            // Filter is something we can push down, replace column references
            // and add to child pushdowner.
            let mut expr = filter.filter;
            replace_references(&plan.node.group_exprs, group_table, &mut expr)?;

            child_pushdown.add_filters([expr]);
        }

        // Replace any filters that we can't push down.
        self.add_filters(remaining_filters.into_iter().map(|f| f.filter));

        // Push down child.
        plan.modify_replace_children(&mut |child| child_pushdown.optimize(bind_context, child))?;

        // Put all remaining filters on top of rewritten agg node.
        self.stop_pushdown(bind_context, LogicalOperator::Aggregate(plan))
    }

    fn pushdown_distinct(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalDistinct>,
    ) -> Result<LogicalOperator> {
        // TODO: This will likely need to be revisited when DISTINCT ON is
        // supported for real.
        plan.modify_replace_children(&mut |child| self.optimize(bind_context, child))?;

        Ok(LogicalOperator::Distinct(plan))
    }

    /// Push down through an ORDER BY.
    ///
    /// No changes needed for the order by node.
    fn pushdown_order_by(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalOrder>,
    ) -> Result<LogicalOperator> {
        // No changes needed for this node, just pass the filter(s) through.
        plan.modify_replace_children(&mut |child| self.optimize(bind_context, child))?;

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
        self.add_filters([plan.node.filter]);
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
                self.add_filters([plan.node.condition]);

                let plan = Node {
                    node: LogicalCrossJoin,
                    location: plan.location,
                    children: plan.children,
                    estimated_cardinality: StatisticsValue::Unknown,
                };

                self.pushdown_cross_join(bind_context, plan)
            }
            // TODO: Other optimizations.
            _ => self.stop_pushdown(bind_context, LogicalOperator::ArbitraryJoin(plan)),
        }
    }

    /// Push down through a magic join.
    ///
    /// This assumes that the left side a materialized scan, and the right
    /// contains any number of "magic" materialized scans.
    ///
    /// Since this join "owns" the materialization, we're free to push filters
    /// down through to the materialization since left and right are dependent
    /// on each other, and we do not need to worry about materialized scans
    /// outside of this join.
    ///
    /// We do not make changes to the conditions of this join as they're
    /// referencing correlated columns (and would be found in both the left and
    /// right always). They will always be hash joins.
    fn pushdown_magic_join(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalMagicJoin>,
    ) -> Result<LogicalOperator> {
        match plan.node.join_type {
            JoinType::Left | JoinType::LeftMark { .. } => {
                // Push down filters that reference only the left side.
                //
                // This **requires** the materialization scans on the right to
                // produce deduplicated tuples (on the correlated columns). The
                // "magic" scan enforces this.
                //
                // # Magic
                //
                // A filter may reference a previously correlated column,
                // however the decorrelation step only exposes the original
                // reference on the left side. The right side exposes new column
                // references (which corresponds to the same underlying
                // correlated column).
                //
                // By having scan on the left keep the original column
                // references, and the scans on right exposing new ones, filters
                // that reference the original correlated column will be seen to
                // only apply to the left side (and thus we're able push into
                // the materialization). The filters are implicitly applied to
                // the right by way of the "magic" scans referencing the same
                // materialization.

                let mut left_pushdown = Self::default();
                let mut right_pushdown = Self::default();

                let [left, right] = plan.take_two_children_exact()?;

                let left_tables = left.get_output_table_refs(bind_context);
                let right_tables = match plan.node.join_type {
                    JoinType::Left => right.get_output_table_refs(bind_context),
                    JoinType::LeftMark { table_ref } => vec![table_ref], // Right side is only able to reference the mark column.
                    _ => unreachable!("join type checked in outer match"),
                };

                let mut remaining_filters = Vec::new();

                for filter in self.drain_filters() {
                    let side = ExprJoinSide::try_from_table_refs(
                        &filter.table_refs,
                        &left_tables,
                        &right_tables,
                    )?;

                    match side {
                        ExprJoinSide::Left => {
                            // Filter should be pushed to materialization.
                            left_pushdown.add_filters([filter.filter]);
                        }
                        _ => remaining_filters.push(filter),
                    }
                }
                self.add_filters(remaining_filters.into_iter().map(|f| f.filter));

                match &left {
                    LogicalOperator::MaterializationScan(scan) => {
                        // Sanity check.
                        if scan.node.mat != plan.node.mat_ref {
                            return Err(DbError::new(format!(
                                "Different materialization refs: {} and {}",
                                scan.node.mat, plan.node.mat_ref
                            )));
                        }

                        let orig = {
                            let mat = &mut bind_context.get_materialization_mut(scan.node.mat)?;
                            std::mem::replace(&mut mat.plan, LogicalOperator::Invalid)
                        };

                        let optimized = left_pushdown.optimize(bind_context, orig)?;

                        let mat = bind_context.get_materialization_mut(scan.node.mat)?;
                        mat.plan = optimized;
                    }
                    other => {
                        return Err(DbError::new(format!(
                            "Unexpected operator on left side of magic join: {other:?}"
                        )));
                    }
                }

                let new_right = right_pushdown.optimize(bind_context, right)?;

                self.stop_pushdown(
                    bind_context,
                    LogicalOperator::MagicJoin(Node {
                        node: LogicalMagicJoin {
                            mat_ref: plan.node.mat_ref,
                            join_type: plan.node.join_type,
                            conditions: plan.node.conditions,
                        },
                        location: plan.location,
                        children: vec![left, new_right], // Left doesn't change, just a reference to a materialization.
                        estimated_cardinality: StatisticsValue::Unknown,
                    }),
                )
            }
            // TODO: Left mark
            _ => self.stop_pushdown(bind_context, LogicalOperator::MagicJoin(plan)),
        }
    }

    /// Pushdown through a comparison join.
    ///
    /// INNER: Turn the join into a cross join + filters. Then push down through
    /// cross join. The provides the opportunity for pushing down conditions
    /// further in the query along with adding more conditions to the result
    /// join.
    ///
    /// LEFT: Does not modify the join, but may push down additional filters to
    /// the left side.
    fn pushdown_comparison_join(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalComparisonJoin>,
    ) -> Result<LogicalOperator> {
        match plan.node.join_type {
            JoinType::Inner => {
                // Convert to cross join, push down on cross join.
                for cond in plan.node.conditions {
                    self.add_filters([Expression::Comparison(cond)]);
                }

                let plan = Node {
                    node: LogicalCrossJoin,
                    location: plan.location,
                    children: plan.children,
                    estimated_cardinality: StatisticsValue::Unknown,
                };

                self.pushdown_cross_join(bind_context, plan)
            }
            JoinType::Left | JoinType::LeftMark { .. } => {
                let mut left_pushdown = Self::default();
                let mut right_pushdown = Self::default();

                let [mut left, mut right] = plan.take_two_children_exact()?;

                let left_tables = left.get_output_table_refs(bind_context);
                let right_tables = match plan.node.join_type {
                    JoinType::Left => right.get_output_table_refs(bind_context),
                    JoinType::LeftMark { table_ref } => vec![table_ref], // Exprs can only reference the mark column if left mark.
                    _ => unreachable!("join type checked in outer match"),
                };

                let mut remaining_filters = Vec::new();

                for filter in self.drain_filters() {
                    let side = ExprJoinSide::try_from_table_refs(
                        &filter.table_refs,
                        &left_tables,
                        &right_tables,
                    )?;

                    // Can only push filters to left side.
                    if side == ExprJoinSide::Left {
                        left_pushdown.add_filters([filter.filter]);
                        continue;
                    }

                    // If the filter expression is referencing the right
                    // side, and the filter only has a single column ref, we
                    // can convert this to a semi join and omit the filter.
                    if side == ExprJoinSide::Right
                        && matches!(plan.node.join_type, JoinType::LeftMark { .. })
                        && matches!(filter.filter, Expression::Column(_))
                    {
                        plan.node.join_type = JoinType::LeftSemi;
                        continue;
                    }

                    remaining_filters.push(filter);
                }

                // Put back remaining filters.
                self.add_filters(remaining_filters.into_iter().map(|f| f.filter));

                // Left/right pushdown.
                left = left_pushdown.optimize(bind_context, left)?;
                right = right_pushdown.optimize(bind_context, right)?;

                plan.children = vec![left, right];

                self.stop_pushdown(bind_context, LogicalOperator::ComparisonJoin(plan))
            }
            // TODO: Other optimizations.
            _ => self.stop_pushdown(bind_context, LogicalOperator::ComparisonJoin(plan)),
        }
    }

    /// Push down through a cross join.
    ///
    /// This will attempt to turn the cross join into an INNER join. Any
    /// remaining filters will be be placed in a filter node just above the
    /// resulting join.
    fn pushdown_cross_join(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: Node<LogicalCrossJoin>,
    ) -> Result<LogicalOperator> {
        if self.filter_gen.is_empty() {
            // Nothing to possible join on.
            return Ok(LogicalOperator::CrossJoin(plan));
        }

        let mut left_pushdown = Self::default();
        let mut right_pushdown = Self::default();

        let [mut left, mut right] = plan.take_two_children_exact()?;

        let left_tables = left.get_output_table_refs(bind_context);
        let right_tables = right.get_output_table_refs(bind_context);

        let mut join_exprs = Vec::new();

        // Figure out which expressions we can push further down vs which are
        // part of the join expression.
        for filter in self.drain_filters() {
            let side =
                ExprJoinSide::try_from_table_refs(&filter.table_refs, &left_tables, &right_tables)?;

            match side {
                ExprJoinSide::Left => {
                    // Filter only depends on left input.
                    left_pushdown.add_filters([filter.filter]);
                }
                ExprJoinSide::Right => {
                    // Filter only depends on right input.
                    right_pushdown.add_filters([filter.filter]);
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
                estimated_cardinality: StatisticsValue::Unknown,
            }));
        }

        // Extract join conditions.
        let extractor = JoinConditionExtractor::new(&left_tables, &right_tables, JoinType::Inner);
        let conditions = extractor.extract(join_exprs)?;

        // We're attempting to do an INNER join and we've already pulled out
        // filters that get pushed to the left/right ops. Both of these should
        // be empty.
        if !conditions.left_filter.is_empty() {
            return Err(DbError::new("Left filters unexpectedly has expression"));
        }
        if !conditions.right_filter.is_empty() {
            return Err(DbError::new("Right filters unexpectedly has expression"));
        }

        // Create the join using the extracted conditions.
        //
        // This will handle creating the join + filter if needed.
        FromPlanner.plan_join_from_conditions(
            JoinType::Inner,
            conditions.comparisons,
            conditions.arbitrary,
            left,
            right,
        )
    }
}

/// Recursively replaces column references in `expr` with the underlying column
/// expression via cloning.
///
/// This expects all column references in `expr` to be pointing to the same
/// table ref.
fn replace_references(
    columns: &[Expression],
    table_ref: TableRef,
    expr: &mut Expression,
) -> Result<()> {
    match expr {
        Expression::Column(col) => {
            if col.reference.table_scope != table_ref {
                return Err(DbError::new(format!(
                    "Unexpected table ref, expected {}, got {}",
                    table_ref, col.reference.table_scope
                )));
            }
            if col.reference.column >= columns.len() {
                return Err(DbError::new(format!(
                    "Column reference outside of expected columns, ref: {col}, columns len: {}",
                    columns.len()
                )));
            }

            *expr = columns[col.reference.column].clone();

            Ok(())
        }
        other => {
            other.for_each_child_mut(&mut |child| replace_references(columns, table_ref, child))
        }
    }
}
