use crate::{
    expr::{
        aggregate_expr::AggregateExpr,
        column_expr::ColumnExpr,
        comparison_expr::{ComparisonExpr, ComparisonOperator},
        literal_expr::LiteralExpr,
        subquery_expr::{SubqueryExpr, SubqueryType},
        Expression,
    },
    functions::aggregate::count::CountNonNullImpl,
    logical::{
        binder::bind_context::BindContext,
        logical_aggregate::LogicalAggregate,
        logical_join::LogicalCrossJoin,
        logical_limit::LogicalLimit,
        logical_project::LogicalProject,
        operator::{LocationRequirement, LogicalNode, LogicalOperator, Node},
        planner::plan_query::QueryPlanner,
    },
};
use rayexec_bullet::{datatype::DataType, scalar::ScalarValue};
use rayexec_error::{not_implemented, Result};

#[derive(Debug)]
pub struct SubqueryPlanner;

impl SubqueryPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        expr: &mut Expression,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.plan_inner(bind_context, expr, &mut plan)?;
        Ok(plan)
    }

    fn plan_inner(
        &self,
        bind_context: &mut BindContext,
        expr: &mut Expression,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        match expr {
            Expression::Subquery(subquery) => {
                if subquery.has_correlations(bind_context)? {
                    not_implemented!("correlated subqueries");
                } else {
                    *expr = self.plan_uncorrelated(bind_context, subquery, plan)?
                }
            }
            other => other.for_each_child_mut(&mut |expr| {
                self.plan_inner(bind_context, expr, plan)?;
                Ok(())
            })?,
        }

        Ok(())
    }

    fn plan_uncorrelated(
        &self,
        bind_context: &mut BindContext,
        subquery: &mut SubqueryExpr,
        plan: &mut LogicalOperator,
    ) -> Result<Expression> {
        // Generate subquery logical plan.
        let subquery_plan = QueryPlanner.plan(bind_context, subquery.subquery.as_ref().clone())?;

        match subquery.subquery_type {
            SubqueryType::Scalar => {
                // Normal subquery.
                //
                // Cross join the subquery with the original input, replace
                // the subquery expression with a reference to the new
                // column.

                // Generate column expr that references the scalar being joined
                // to the plan.
                let subquery_table = subquery_plan.get_output_table_refs()[0];
                let column = ColumnExpr {
                    table_scope: subquery_table,
                    column: 0,
                };

                // Limit original subquery to only one row.
                let subquery_plan = LogicalOperator::Limit(Node {
                    node: LogicalLimit {
                        offset: None,
                        limit: 1,
                    },
                    location: LocationRequirement::Any,
                    children: vec![subquery_plan],
                });

                // Cross join!
                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                *plan = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![orig, subquery_plan],
                });

                Ok(Expression::Column(column))
            }
            SubqueryType::Exists { negated } => {
                // Exists subquery.
                //
                // EXISTS -> COUNT(*) == 1
                // NOT EXISTS -> COUNT(*) != 1
                //
                // Cross join with existing input. Replace original subquery expression
                // with reference to new column.

                let subquery_table = subquery_plan.get_output_table_refs()[0];
                let subquery_column = ColumnExpr {
                    table_scope: subquery_table,
                    column: 0,
                };

                let agg_table = bind_context.new_ephemeral_table()?;
                bind_context.push_column_for_table(
                    agg_table,
                    "__generated_count",
                    DataType::Int64,
                )?;

                let projection_table = bind_context.new_ephemeral_table()?;
                bind_context.push_column_for_table(
                    projection_table,
                    "__generated_exists",
                    DataType::Boolean,
                )?;

                let subquery_exists_plan = LogicalOperator::Project(Node {
                    node: LogicalProject {
                        projections: vec![Expression::Comparison(ComparisonExpr {
                            left: Box::new(Expression::Column(ColumnExpr {
                                table_scope: agg_table,
                                column: 0,
                            })),
                            right: Box::new(Expression::Literal(LiteralExpr {
                                literal: ScalarValue::Int64(1),
                            })),
                            op: if negated {
                                ComparisonOperator::NotEq
                            } else {
                                ComparisonOperator::Eq
                            },
                        })],
                        projection_table,
                    },
                    location: LocationRequirement::Any,
                    children: vec![LogicalOperator::Aggregate(Node {
                        node: LogicalAggregate {
                            aggregates_table: agg_table,
                            aggregates: vec![Expression::Aggregate(AggregateExpr {
                                agg: Box::new(CountNonNullImpl),
                                inputs: vec![Expression::Column(subquery_column)],
                                filter: None,
                            })],
                            group_table: None,
                            group_exprs: Vec::new(),
                            grouping_sets: None,
                        },
                        location: LocationRequirement::Any,
                        children: vec![LogicalOperator::Limit(Node {
                            node: LogicalLimit {
                                offset: None,
                                limit: 1,
                            },
                            location: LocationRequirement::Any,
                            children: vec![subquery_plan],
                        })],
                    })],
                });

                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                *plan = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![orig, subquery_exists_plan],
                });

                // Return column referencing the project.
                Ok(Expression::Column(ColumnExpr {
                    table_scope: projection_table,
                    column: 0,
                }))
            }
            other => not_implemented!("subquery type {other:?}"),
        }
    }
}
