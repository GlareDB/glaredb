use glaredb_error::Result;

use super::plan_unnest::UnnestPlanner;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_query::bind_select::BoundSelect;
use crate::logical::binder::bind_query::select_list::BoundDistinctModifier;
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::logical_distinct::LogicalDistinct;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_limit::LogicalLimit;
use crate::logical::logical_order::LogicalOrder;
use crate::logical::logical_project::LogicalProject;
use crate::logical::logical_window::LogicalWindow;
use crate::logical::operator::{LocationRequirement, LogicalOperator, Node};
use crate::logical::planner::plan_from::FromPlanner;
use crate::logical::planner::plan_subquery::SubqueryPlanner;
use crate::logical::statistics::StatisticsValue;

#[derive(Debug)]
pub struct SelectPlanner;

impl SelectPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        mut select: BoundSelect,
    ) -> Result<LogicalOperator> {
        // Handle FROM
        let mut plan = FromPlanner.plan(bind_context, select.from)?;

        // Handle WHERE
        if let Some(mut filter) = select.filter {
            plan = SubqueryPlanner.plan_expression(bind_context, &mut filter, plan)?;
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            });
        }

        // Handle GROUP BY/aggregates
        if !select.select_list.aggregates.is_empty() || select.group_by.is_some() {
            let (mut group_exprs, group_table, grouping_sets) = match select.group_by {
                Some(group_by) => (
                    group_by.expressions,
                    Some(group_by.group_exprs_table),
                    Some(group_by.grouping_sets),
                ),
                None => (Vec::new(), None, None),
            };

            for expr in &mut group_exprs {
                plan = SubqueryPlanner.plan_expression(bind_context, expr, plan)?;
            }

            for expr in &mut select.select_list.aggregates {
                plan = SubqueryPlanner.plan_expression(bind_context, expr, plan)?;
            }

            let (grouping_functions, grouping_functions_table) =
                if select.select_list.grouping_functions.is_empty() {
                    (Vec::new(), None)
                } else {
                    (
                        select.select_list.grouping_functions,
                        Some(select.select_list.grouping_functions_table),
                    )
                };

            let agg = LogicalAggregate {
                aggregates_table: select.select_list.aggregates_table,
                aggregates: select.select_list.aggregates,
                group_exprs,
                group_table,
                grouping_sets,
                grouping_functions_table,
                grouping_functions,
            };

            plan = LogicalOperator::Aggregate(Node {
                node: agg,
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            });
            plan = UnnestPlanner.plan_unnests(bind_context, plan)?;
        }

        // Handle HAVING
        if let Some(mut expr) = select.having {
            plan = SubqueryPlanner.plan_expression(bind_context, &mut expr, plan)?;
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter: expr },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        // Handle windows
        if !select.select_list.windows.is_empty() {
            for expr in &mut select.select_list.windows {
                plan = SubqueryPlanner.plan_expression(bind_context, expr, plan)?;
            }
            plan = LogicalOperator::Window(Node {
                node: LogicalWindow {
                    windows: select.select_list.windows,
                    windows_table: select.select_list.windows_table,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            });
        }

        // Handle projections.
        for expr in &mut select.select_list.projections {
            plan = SubqueryPlanner.plan_expression(bind_context, expr, plan)?;
        }

        plan = LogicalOperator::Project(Node {
            node: LogicalProject {
                projections: select.select_list.projections,
                projection_table: select.select_list.projections_table,
            },
            location: LocationRequirement::Any,
            children: vec![plan],
            estimated_cardinality: StatisticsValue::Unknown,
        });
        // Handle possible UNNESTing from the projection.
        plan = UnnestPlanner.plan_unnests(bind_context, plan)?;

        // Handle DISTINCT. Note this comes after the UNNESTing since we'd want
        // to distinct the results of the unnested output.
        if select.select_list.distinct_modifier == BoundDistinctModifier::Distinct {
            plan = LogicalOperator::Distinct(Node {
                node: LogicalDistinct {},
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        // Handle ORDER BY
        if let Some(order_by) = select.order_by {
            plan = LogicalOperator::Order(Node {
                node: LogicalOrder {
                    exprs: order_by.exprs,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        // Handle LIMIT
        if let Some(limit) = select.limit {
            plan = LogicalOperator::Limit(Node {
                node: LogicalLimit {
                    offset: limit.offset,
                    limit: limit.limit,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            });
        }

        // Omit any columns that shouldn't be in the output.
        if let Some(output) = select.select_list.output {
            plan = LogicalOperator::Project(Node {
                node: LogicalProject {
                    projections: output.expressions,
                    projection_table: output.table,
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        Ok(plan)
    }
}
