use rayexec_error::Result;

use super::plan_unnest::UnnestPlanner;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_query::bind_select::BoundSelect;
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_limit::LogicalLimit;
use crate::logical::logical_order::LogicalOrder;
use crate::logical::logical_project::LogicalProject;
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
            plan = SubqueryPlanner.plan(bind_context, &mut filter, plan)?;
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
                    Some(group_by.group_table),
                    Some(group_by.grouping_sets),
                ),
                None => (Vec::new(), None, None),
            };

            for expr in &mut group_exprs {
                plan = SubqueryPlanner.plan(bind_context, expr, plan)?;
            }

            for expr in &mut select.select_list.aggregates {
                plan = SubqueryPlanner.plan(bind_context, expr, plan)?;
            }

            let agg = LogicalAggregate {
                aggregates_table: select.select_list.aggregates_table,
                aggregates: select.select_list.aggregates,
                group_exprs,
                group_table,
                grouping_sets,
                grouping_set_table: None,
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
            plan = SubqueryPlanner.plan(bind_context, &mut expr, plan)?;
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter: expr },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        // Handle projections.
        for expr in &mut select.select_list.projections {
            plan = SubqueryPlanner.plan(bind_context, expr, plan)?;
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
        // Handle possible UNNESTing.
        plan = UnnestPlanner.plan_unnests(bind_context, plan)?;

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
