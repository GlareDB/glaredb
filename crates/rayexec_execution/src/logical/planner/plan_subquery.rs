use std::collections::{BTreeSet, HashMap};

use rayexec_error::{not_implemented, RayexecError, Result};

use crate::arrays::datatype::DataType;
use crate::expr::aggregate_expr::AggregateExpr;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::negate_expr::NegateOperator;
use crate::expr::subquery_expr::{SubqueryExpr, SubqueryType};
use crate::expr::{self, Expression};
use crate::functions::aggregate::builtin::count::Count;
use crate::functions::aggregate::AggregateFunction;
use crate::logical::binder::bind_context::{BindContext, CorrelatedColumn, MaterializationRef};
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::logical_join::{
    JoinType,
    LogicalComparisonJoin,
    LogicalCrossJoin,
    LogicalMagicJoin,
};
use crate::logical::logical_limit::LogicalLimit;
use crate::logical::logical_materialization::{
    LogicalMagicMaterializationScan,
    LogicalMaterializationScan,
};
use crate::logical::logical_project::LogicalProject;
use crate::logical::logical_scan::ScanSource;
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::planner::plan_query::QueryPlanner;
use crate::logical::statistics::StatisticsValue;

#[derive(Debug)]
pub struct SubqueryPlanner;

impl SubqueryPlanner {
    /// Plans a subquery expression.
    pub fn plan_expression(
        &self,
        bind_context: &mut BindContext,
        expr: &mut Expression,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.plan_expression_inner(bind_context, expr, &mut plan)?;
        Ok(plan)
    }

    pub fn plan_lateral_join(
        &self,
        bind_context: &mut BindContext,
        left: LogicalOperator,
        mut right: LogicalOperator,
        join_type: JoinType,
        mut conditions: Vec<ComparisonExpr>,
        lateral_columns: Vec<CorrelatedColumn>,
    ) -> Result<LogicalOperator> {
        // Very similar to planning correlated subqueries (becuase it is), just
        // we already have the correlated columns we're flattening for.

        // Materialize left.
        let mat_ref = bind_context.new_materialization(left)?;
        let left = LogicalOperator::MaterializationScan(Node {
            node: LogicalMaterializationScan { mat: mat_ref },
            location: LocationRequirement::Any,
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        });
        bind_context.inc_materialization_scan_count(mat_ref, 1)?;

        // Flatten right side.
        let mut planner = DependentJoinPushdown::new(mat_ref, lateral_columns);
        planner.find_correlations(&right)?;
        planner.pushdown(bind_context, &mut right)?;

        // Generate additional conditions.
        for correlated in planner.columns {
            // Correlated points to left, the materialized side.
            let left_reference = ColumnReference {
                table_scope: correlated.table,
                column: correlated.col_idx,
            };
            let left = Expression::Column(ColumnExpr {
                reference: left_reference,
                datatype: bind_context.get_column_type(left_reference)?,
            });

            let right = *planner.column_map.get(&correlated).ok_or_else(|| {
                RayexecError::new(format!(
                    "Missing updated right side for correlate column: {correlated:?}"
                ))
            })?;

            let condition = expr::compare(
                ComparisonOperator::Eq,
                left,
                Expression::Column(ColumnExpr {
                    reference: right,
                    datatype: bind_context.get_column_type(right)?,
                }),
            )?;

            conditions.push(condition);
        }

        // Produce the output join with additional comparison conditions.
        Ok(LogicalOperator::MagicJoin(Node {
            node: LogicalMagicJoin {
                mat_ref,
                join_type,
                conditions,
            },
            location: LocationRequirement::Any,
            children: vec![left, right],
            estimated_cardinality: StatisticsValue::Unknown,
        }))
    }

    fn plan_expression_inner(
        &self,
        bind_context: &mut BindContext,
        expr: &mut Expression,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        match expr {
            Expression::Subquery(subquery) => {
                if subquery.has_correlations(bind_context)? {
                    *expr = self.plan_correlated(bind_context, subquery, plan)?
                } else {
                    *expr = self.plan_uncorrelated(bind_context, subquery, plan)?
                }
            }
            other => other.for_each_child_mut(&mut |expr| {
                self.plan_expression_inner(bind_context, expr, plan)?;
                Ok(())
            })?,
        }

        Ok(())
    }

    /// Plans a correlated subquery.
    ///
    /// This will attempt to decorrelate the subquery, modifying `plan` to do
    /// so. The returned expression should then be used in place of the original
    /// subquery expression.
    ///
    /// Decorrelation follows the logic described in "Unnesting Arbitrary
    /// Queries" (Neumann, Kemper):
    ///
    /// <https://btw-2015.informatik.uni-hamburg.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf>
    fn plan_correlated(
        &self,
        bind_context: &mut BindContext,
        subquery: &mut SubqueryExpr,
        plan: &mut LogicalOperator,
    ) -> Result<Expression> {
        let orig = std::mem::replace(plan, LogicalOperator::Invalid);
        let ([left, right], mut conditions, mat_ref) =
            self.plan_left_right_for_correlated(bind_context, subquery, orig)?;

        match &subquery.subquery_type {
            SubqueryType::Scalar => {
                // Result expression for the subquery, output of the right side
                // of the join.
                let right_reference = ColumnReference {
                    table_scope: right.get_output_table_refs(bind_context)[0],
                    column: 0,
                };
                let right_out = Expression::Column(ColumnExpr {
                    reference: right_reference,
                    datatype: bind_context.get_column_type(right_reference)?,
                });

                // Update plan to now be a comparison join.
                *plan = LogicalOperator::MagicJoin(Node {
                    node: LogicalMagicJoin {
                        mat_ref,
                        join_type: JoinType::Left,
                        conditions,
                    },
                    location: LocationRequirement::Any,
                    children: vec![left, right],
                    estimated_cardinality: StatisticsValue::Unknown,
                });

                Ok(right_out)
            }
            SubqueryType::Exists { negated } => {
                let mark_table = bind_context.new_ephemeral_table()?;
                bind_context.push_column_for_table(
                    mark_table,
                    "__generated_visited_bool",
                    DataType::Boolean,
                )?;

                *plan = LogicalOperator::MagicJoin(Node {
                    node: LogicalMagicJoin {
                        mat_ref,
                        join_type: JoinType::LeftMark {
                            table_ref: mark_table,
                        },
                        conditions,
                    },
                    location: LocationRequirement::Any,
                    children: vec![left, right],
                    estimated_cardinality: StatisticsValue::Unknown,
                });

                let mut visited_expr = Expression::Column(ColumnExpr {
                    reference: ColumnReference {
                        table_scope: mark_table,
                        column: 0,
                    },
                    datatype: DataType::Boolean,
                });

                if *negated {
                    visited_expr = expr::negate(NegateOperator::Not, visited_expr)?.into();
                }

                Ok(visited_expr)
            }
            SubqueryType::Any { expr, op } => {
                // Similar to EXISTS, just with an extra join condition
                // representing the ANY condition.

                let right_reference = ColumnReference {
                    table_scope: right.get_output_table_refs(bind_context)[0],
                    column: 0,
                };
                let right_out = Expression::Column(ColumnExpr {
                    reference: right_reference,
                    datatype: bind_context.get_column_type(right_reference)?,
                });

                let mark_table = bind_context.new_ephemeral_table()?;
                bind_context.push_column_for_table(
                    mark_table,
                    "__generated_visited_bool",
                    DataType::Boolean,
                )?;

                let condition = expr::compare(*op, expr.as_ref().clone(), right_out)?;
                conditions.push(condition);

                *plan = LogicalOperator::MagicJoin(Node {
                    node: LogicalMagicJoin {
                        mat_ref,
                        join_type: JoinType::LeftMark {
                            table_ref: mark_table,
                        },
                        conditions,
                    },
                    location: LocationRequirement::Any,
                    children: vec![left, right],
                    estimated_cardinality: StatisticsValue::Unknown,
                });

                Ok(Expression::Column(ColumnExpr {
                    reference: ColumnReference {
                        table_scope: mark_table,
                        column: 0,
                    },
                    datatype: DataType::Boolean,
                }))
            }
        }
    }

    /// Plans the left and right side of a join for decorrelated a subquery.
    ///
    /// This will place `plan` in a materialization.
    // TODO: Return is gnarly.
    fn plan_left_right_for_correlated(
        &self,
        bind_context: &mut BindContext,
        subquery: &mut SubqueryExpr,
        plan: LogicalOperator,
    ) -> Result<(
        [LogicalOperator; 2],
        Vec<ComparisonExpr>,
        MaterializationRef,
    )> {
        let mut subquery_plan =
            QueryPlanner.plan(bind_context, subquery.subquery.as_ref().clone())?;

        // Get only correlated columns that are pointing to this plan.
        let plan_tables = plan.get_output_table_refs(bind_context);

        let correlated_columns: Vec<_> = bind_context
            .correlated_columns(subquery.bind_idx)?
            .iter()
            .filter(|c| plan_tables.contains(&c.table))
            .cloned()
            .collect();

        // Create dependent join between left (original query) and right
        // (subquery). Left requires duplication elimination on the
        // correlated columns.
        //
        // The resulting plan may have nodes scanning from the left
        // multiple times.

        let mat_ref = bind_context.new_materialization(plan)?;

        let left = LogicalOperator::MaterializationScan(Node {
            node: LogicalMaterializationScan { mat: mat_ref },
            location: LocationRequirement::Any,
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        });
        bind_context.inc_materialization_scan_count(mat_ref, 1)?;

        // Flatten the right side. This assumes we're doing a dependent
        // join with left. The goal is after flattening here, the join
        // we make at the end _shouldn't_ be a dependent join, but just
        // a normal comparison join.
        let mut planner = DependentJoinPushdown::new(mat_ref, correlated_columns);

        planner.find_correlations(&subquery_plan)?;
        planner.pushdown(bind_context, &mut subquery_plan)?;

        // Make comparison join between left & right using the updated
        // column map from the push down.

        let mut conditions = Vec::with_capacity(planner.columns.len());
        for correlated in planner.columns {
            // Correlated points to left, the materialized side.
            let left_reference = ColumnReference {
                table_scope: correlated.table,
                column: correlated.col_idx,
            };
            let left = Expression::Column(ColumnExpr {
                reference: left_reference,
                datatype: bind_context.get_column_type(left_reference)?,
            });

            let right = *planner.column_map.get(&correlated).ok_or_else(|| {
                RayexecError::new(format!(
                    "Missing updated right side for correlate column: {correlated:?}"
                ))
            })?;

            let condition = expr::compare(
                ComparisonOperator::Eq,
                left,
                Expression::Column(ColumnExpr {
                    reference: right,
                    datatype: bind_context.get_column_type(right)?,
                }),
            )?;
            conditions.push(condition);
        }

        Ok(([left, subquery_plan], conditions, mat_ref))
    }

    fn plan_uncorrelated(
        &self,
        bind_context: &mut BindContext,
        subquery: &mut SubqueryExpr,
        plan: &mut LogicalOperator,
    ) -> Result<Expression> {
        // Generate subquery logical plan.
        let subquery_plan = QueryPlanner.plan(bind_context, subquery.subquery.as_ref().clone())?;

        match &subquery.subquery_type {
            SubqueryType::Scalar => {
                // Normal subquery.
                //
                // Cross join the subquery with the original input, replace
                // the subquery expression with a reference to the new
                // column.

                // Generate column expr that references the scalar being joined
                // to the plan.
                let subquery_table = subquery_plan.get_output_table_refs(bind_context)[0];
                let subquery_col_reference = ColumnReference {
                    table_scope: subquery_table,
                    column: 0,
                };
                let column = ColumnExpr {
                    reference: subquery_col_reference,
                    datatype: bind_context.get_column_type(subquery_col_reference)?,
                };

                // Limit original subquery to only one row.
                let subquery_plan = LogicalOperator::Limit(Node {
                    node: LogicalLimit {
                        offset: None,
                        limit: 1,
                    },
                    location: LocationRequirement::Any,
                    children: vec![subquery_plan],
                    estimated_cardinality: StatisticsValue::Unknown,
                });

                // Cross join!
                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                *plan = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![orig, subquery_plan],
                    estimated_cardinality: StatisticsValue::Unknown,
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

                let subquery_table = subquery_plan.get_output_table_refs(bind_context)[0];
                let subquery_col_ref = ColumnReference {
                    table_scope: subquery_table,
                    column: 0,
                };
                let subquery_column = ColumnExpr {
                    reference: subquery_col_ref,
                    datatype: bind_context.get_column_type(subquery_col_ref)?,
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

                let cmp_op = if *negated {
                    ComparisonOperator::NotEq
                } else {
                    ComparisonOperator::Eq
                };

                let projection = expr::compare(
                    cmp_op,
                    expr::column((agg_table, 0), DataType::Int64),
                    expr::lit(1_i64),
                )?;

                let subquery_exists_plan = LogicalOperator::Project(Node {
                    node: LogicalProject {
                        projections: vec![projection.into()],
                        projection_table,
                    },
                    location: LocationRequirement::Any,
                    children: vec![LogicalOperator::Aggregate(Node {
                        node: LogicalAggregate {
                            aggregates_table: agg_table,
                            aggregates: vec![Expression::Aggregate(AggregateExpr {
                                agg: Count.plan(
                                    bind_context.get_table_list(),
                                    vec![Expression::Column(subquery_column)],
                                )?,
                                distinct: false,
                                filter: None,
                            })],
                            group_table: None,
                            group_exprs: Vec::new(),
                            grouping_sets: None,
                            grouping_functions_table: None,
                            grouping_functions: Vec::new(),
                        },
                        location: LocationRequirement::Any,
                        children: vec![LogicalOperator::Limit(Node {
                            node: LogicalLimit {
                                offset: None,
                                limit: 1,
                            },
                            location: LocationRequirement::Any,
                            children: vec![subquery_plan],
                            estimated_cardinality: StatisticsValue::Unknown,
                        })],
                        estimated_cardinality: StatisticsValue::Unknown,
                    })],
                    estimated_cardinality: StatisticsValue::Unknown,
                });

                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                *plan = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![orig, subquery_exists_plan],
                    estimated_cardinality: StatisticsValue::Unknown,
                });

                // Return column referencing the project.
                Ok(Expression::Column(ColumnExpr {
                    reference: ColumnReference {
                        table_scope: projection_table,
                        column: 0,
                    },
                    datatype: DataType::Boolean,
                }))
            }
            SubqueryType::Any { expr, op } => {
                // Any subquery.
                //
                // Join original plan (left) with subquery (right) with
                // comparison referencing left/right sides.
                //
                // Resulting expression is boolean indicating if there was join
                // between left and right.

                let mark_table = bind_context.new_ephemeral_table()?;
                bind_context.push_column_for_table(
                    mark_table,
                    "__generated_visited_bool",
                    DataType::Boolean,
                )?;

                let subquery_table = subquery_plan.get_output_table_refs(bind_context)[0];
                let subquery_col_ref = ColumnReference {
                    table_scope: subquery_table,
                    column: 0,
                };
                let column = ColumnExpr {
                    reference: subquery_col_ref,
                    datatype: bind_context.get_column_type(subquery_col_ref)?,
                };

                let condition =
                    expr::compare(*op, expr.as_ref().clone(), Expression::Column(column))?;

                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                *plan = LogicalOperator::ComparisonJoin(Node {
                    node: LogicalComparisonJoin {
                        join_type: JoinType::LeftMark {
                            table_ref: mark_table,
                        },
                        conditions: vec![condition],
                    },
                    location: LocationRequirement::Any,
                    children: vec![orig, subquery_plan],
                    estimated_cardinality: StatisticsValue::Unknown,
                });

                Ok(Expression::Column(ColumnExpr {
                    reference: ColumnReference {
                        table_scope: mark_table,
                        column: 0,
                    },
                    datatype: DataType::Boolean,
                }))
            }
        }
    }
}

/// Wrapper around a logical operator pointer for hashing the pointer.
///
/// This is used to allow us to walk the plan determining if subtrees contain
/// correlated queries without needing to store the operator.
///
/// This may or may not be smart. I don't know yet.
#[derive(Debug)]
struct LogicalOperatorPtr(*const LogicalOperator);

impl LogicalOperatorPtr {
    fn new(plan: &LogicalOperator) -> Self {
        LogicalOperatorPtr(plan as _)
    }
}

impl std::hash::Hash for LogicalOperatorPtr {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        self.0.hash(state)
    }
}

impl PartialEq<LogicalOperatorPtr> for LogicalOperatorPtr {
    fn eq(&self, other: &LogicalOperatorPtr) -> bool {
        self.0 == other.0
    }
}

impl Eq for LogicalOperatorPtr {}

/// Contains logic for pushing down a dependent join in a logical such that the
/// resulting plan does not have a dependent join.
#[derive(Debug)]
struct DependentJoinPushdown {
    /// Reference to the materialized plan on the left side.
    mat_ref: MaterializationRef,
    /// Holds pointers to nodes in the plan indicating if it or any of its
    /// children contains a correlated column.
    correlated_operators: HashMap<LogicalOperatorPtr, bool>,
    /// Map correlated columns to updated column expressions.
    ///
    /// This is updated as we walk back up the plan to allow expressions further
    /// up the tree to be rewritten to point to now decorrelated columns.
    column_map: HashMap<CorrelatedColumn, ColumnReference>,
    /// Set of correlated columns we're looking for in the plan.
    columns: BTreeSet<CorrelatedColumn>,
}

impl DependentJoinPushdown {
    /// Creates a new dependent join pushdown for pushing down the given
    /// correlated columns.
    ///
    /// This will deduplicate correlated columns.
    fn new(
        mat_ref: MaterializationRef,
        columns: impl IntoIterator<Item = CorrelatedColumn>,
    ) -> Self {
        let columns: BTreeSet<_> = columns.into_iter().collect();

        // Initial column map points to itself.
        let column_map: HashMap<_, _> = columns
            .iter()
            .map(|c| {
                (
                    c.clone(),
                    ColumnReference {
                        table_scope: c.table,
                        column: c.col_idx,
                    },
                )
            })
            .collect();

        DependentJoinPushdown {
            mat_ref,
            correlated_operators: HashMap::new(),
            column_map,
            columns,
        }
    }

    /// Walk the logical plan and find correlations that we need to handle
    /// during pushdown.
    fn find_correlations(&mut self, plan: &LogicalOperator) -> Result<bool> {
        let mut has_correlation = false;
        match plan {
            LogicalOperator::Project(project) => {
                has_correlation = self.any_expression_has_correlation(&project.node.projections);
                has_correlation |= self.find_correlations_in_children(&project.children)?;
            }
            LogicalOperator::Filter(filter) => {
                has_correlation = self.expression_has_correlation(&filter.node.filter);
                has_correlation |= self.find_correlations_in_children(&filter.children)?;
            }
            LogicalOperator::Aggregate(agg) => {
                has_correlation = self.any_expression_has_correlation(&agg.node.aggregates);
                has_correlation |= self.any_expression_has_correlation(&agg.node.group_exprs);
                has_correlation |= self.find_correlations_in_children(&agg.children)?;
            }
            LogicalOperator::CrossJoin(join) => {
                // TODO: Implement the push down
                has_correlation = self.find_correlations_in_children(&join.children)?;
            }
            LogicalOperator::ArbitraryJoin(join) => {
                // TODO: Implement the push down
                has_correlation = self.expression_has_correlation(&join.node.condition);
                has_correlation |= self.find_correlations_in_children(&join.children)?
            }
            LogicalOperator::ComparisonJoin(join) => {
                // TODO: Implement the push down
                has_correlation = self.any_expression_has_correlation(
                    join.node
                        .conditions
                        .iter()
                        .flat_map(|c| [c.left.as_ref(), c.right.as_ref()].into_iter()),
                );
                has_correlation |= self.find_correlations_in_children(&join.children)?;
            }
            LogicalOperator::Limit(_) => {
                // Limit should not have correlations.
            }
            LogicalOperator::Order(order) => {
                // TODO: Implement the push down
                has_correlation =
                    self.any_expression_has_correlation(order.node.exprs.iter().map(|e| &e.expr));
                has_correlation |= self.find_correlations_in_children(&order.children)?;
            }
            LogicalOperator::InOut(inout) => {
                has_correlation =
                    self.any_expression_has_correlation(&inout.node.function.positional);
                has_correlation |= self.find_correlations_in_children(&inout.children)?;
            }
            _ => (),
        }

        self.correlated_operators
            .insert(LogicalOperatorPtr::new(plan), has_correlation);

        Ok(has_correlation)
    }

    fn find_correlations_in_children(&mut self, children: &[LogicalOperator]) -> Result<bool> {
        let mut child_has_correlation = false;
        for child in children {
            child_has_correlation |= self.find_correlations(child)?;
        }
        Ok(child_has_correlation)
    }

    /// Pushes down a conceptual dependent join.
    ///
    /// Note that there's no explicit "dependent join" operator, and this is
    /// just acting as if there was one. The resulting plan should contain a
    /// cross join against the materialized original plan, with all correlated
    /// columns resolved against that cross join.
    ///
    /// Also Sean decide that we hash pointers, so this takes a mut reference to
    /// the plan and modify in place instead of returning a new plan. This
    /// reference should be the same one used for `find_correlations`, otherwise
    /// an error occurs.
    fn pushdown(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        let has_correlation = *self
            .correlated_operators
            .get(&LogicalOperatorPtr::new(plan))
            .ok_or_else(|| {
                RayexecError::new(format!("Missing correlation check for plan: {plan:?}"))
            })?;

        if !has_correlation {
            // Operator (and children) do not have correlated columns. Cross
            // join with materialized scan with duplicates eliminated.

            let projection_ref = bind_context.new_ephemeral_table()?;
            let mut projected_cols = Vec::with_capacity(self.columns.len());

            for (idx, correlated) in self.columns.iter().enumerate() {
                // Push a mapping of correlated -> projected materialized column.
                //
                // As we walk back up the tree, the mappings will be updated to
                // point to the appropriate column.
                self.column_map.insert(
                    correlated.clone(),
                    ColumnReference {
                        table_scope: projection_ref,
                        column: idx,
                    },
                );

                let datatype =
                    bind_context.get_column_type((correlated.table, correlated.col_idx))?;

                bind_context.push_column_for_table(
                    projection_ref,
                    format!("__generated_mat_scan_projection_{idx}"),
                    datatype.clone(),
                )?;

                // This uses the original correlated column info since the
                // column should already be pointing to the output of the
                // materialization.
                projected_cols.push(Expression::Column(ColumnExpr {
                    reference: ColumnReference {
                        table_scope: correlated.table,
                        column: correlated.col_idx,
                    },
                    datatype,
                }));
            }

            // Note this scan is reading from the left side of the query, but
            // being placed on the right side of the join. This is to make
            // rewriting operators (projections) further up this subtree easier.
            //
            // For projections, we have to to ensure that there's column exprs
            // that point to the materialized node, and by having the
            // materialization on the right, we can just append the expressions.
            let right = LogicalOperator::MagicMaterializationScan(Node {
                node: LogicalMagicMaterializationScan {
                    mat: self.mat_ref,
                    projections: projected_cols,
                    table_ref: projection_ref,
                },
                location: LocationRequirement::Any,
                children: Vec::new(),
                estimated_cardinality: StatisticsValue::Unknown,
            });
            bind_context.inc_materialization_scan_count(self.mat_ref, 1)?;

            let orig = std::mem::replace(plan, LogicalOperator::Invalid);

            *plan = LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![orig, right],
                estimated_cardinality: StatisticsValue::Unknown,
            });

            return Ok(());
        }

        match plan {
            LogicalOperator::Project(project) => {
                self.pushdown_children(bind_context, &mut project.children)?;
                self.rewrite_expressions(bind_context, &mut project.node.projections)?;

                // Append column exprs referencing the materialization.
                let offset = project.node.projections.len();
                for (idx, correlated) in self.columns.iter().enumerate() {
                    let reference = *self.column_map.get(correlated).ok_or_else(|| {
                            RayexecError::new(
                                format!("Missing correlated column in column map for appending projection: {correlated:?}"))
                    })?;

                    let datatype = bind_context.get_column_type(reference)?;
                    let expr = Expression::Column(ColumnExpr {
                        reference,
                        datatype: datatype.clone(),
                    });

                    // Append column to table in bind context.
                    bind_context.push_column_for_table(
                        project.node.projection_table,
                        format!("__generated_projection_decorrelation_{idx}"),
                        datatype,
                    )?;

                    project.node.projections.push(expr);

                    self.column_map.insert(
                        correlated.clone(),
                        ColumnReference {
                            table_scope: project.node.projection_table,
                            column: offset + idx,
                        },
                    );
                }

                Ok(())
            }
            LogicalOperator::InOut(inout) => {
                self.pushdown_children(bind_context, &mut inout.children)?;
                self.rewrite_expressions(bind_context, &mut inout.node.function.positional)?;

                // Add projections table as needed.
                let table_ref = match inout.node.projected_table_ref {
                    Some(table_ref) => table_ref, // TODO: List out how this could be Some already
                    None => {
                        let table_ref = bind_context.new_ephemeral_table()?;
                        inout.node.projected_table_ref = Some(table_ref);
                        table_ref
                    }
                };

                // Append correlated columns to output projections.
                let offset = inout.node.projected_outputs.len();
                for (idx, correlated) in self.columns.iter().enumerate() {
                    let reference = *self.column_map.get(correlated).ok_or_else(|| {
                            RayexecError::new(
                                format!("Missing correlated column in column map for appending projection to In/Out: {correlated:?}"))
                    })?;

                    let datatype = bind_context.get_column_type(reference)?;
                    let expr = Expression::Column(ColumnExpr {
                        reference,
                        datatype: datatype.clone(),
                    });

                    // Append column to table in bind context.
                    bind_context.push_column_for_table(
                        table_ref,
                        format!("__generated_inout_projection_decorrelation_{idx}"),
                        datatype,
                    )?;

                    inout.node.projected_outputs.push(expr);

                    self.column_map.insert(
                        correlated.clone(),
                        ColumnReference {
                            table_scope: table_ref,
                            column: offset + idx,
                        },
                    );
                }

                Ok(())
            }
            LogicalOperator::Filter(filter) => {
                self.pushdown_children(bind_context, &mut filter.children)?;
                self.rewrite_expression(bind_context, &mut filter.node.filter)?;

                // Filter does not change columns that can be referenced by
                // parent nodes, don't update column map.

                Ok(())
            }
            LogicalOperator::Aggregate(agg) => {
                self.pushdown_children(bind_context, &mut agg.children)?;
                self.rewrite_expressions(bind_context, &mut agg.node.aggregates)?;
                self.rewrite_expressions(bind_context, &mut agg.node.group_exprs)?;

                // Append correlated columns to group by expressions.
                let offset = agg.node.group_exprs.len();

                // If we don't have a table ref for the group by (indicating we
                // have no groups), go ahead and create it.
                let group_by_table = match agg.node.group_table {
                    Some(table) => table,
                    None => {
                        let table = bind_context.new_ephemeral_table()?;
                        agg.node.group_table = Some(table);
                        table
                    }
                };

                // Same as above, we're always going to have groups.
                let grouping_sets = match &mut agg.node.grouping_sets {
                    Some(sets) => sets,
                    None => {
                        // Create single group.
                        agg.node.grouping_sets = Some(vec![BTreeSet::new()]);
                        agg.node.grouping_sets.as_mut().unwrap()
                    }
                };

                for (idx, correlated) in self.columns.iter().enumerate() {
                    let reference = *self.column_map.get(correlated).ok_or_else(|| {
                            RayexecError::new(
                                format!("Missing correlated column in column map for appending group expression: {correlated:?}"))
                    })?;

                    let datatype = bind_context.get_column_type(reference)?;
                    let expr = Expression::Column(ColumnExpr {
                        reference,
                        datatype: datatype.clone(),
                    });

                    // Append column to group by table in bind context.
                    bind_context.push_column_for_table(
                        group_by_table,
                        format!("__generated_aggregate_decorrelation_{idx}"),
                        datatype,
                    )?;

                    // Add to group by.
                    agg.node.group_exprs.push(expr);
                    // Add to all grouping sets too.
                    for set in grouping_sets.iter_mut() {
                        set.insert(offset + idx);
                    }

                    // Update column map to point to expression in GROUP BY.
                    self.column_map.insert(
                        correlated.clone(),
                        ColumnReference {
                            table_scope: group_by_table,
                            column: offset + idx,
                        },
                    );
                }

                Ok(())
            }
            LogicalOperator::Scan(scan) => {
                if matches!(
                    scan.node.source,
                    ScanSource::Table { .. }
                        | ScanSource::View { .. }
                        | ScanSource::TableFunction { .. }
                ) {
                    return Err(RayexecError::new(
                        "Unexpectedly reached scan node when pushing down dependent join",
                    ));
                }

                not_implemented!("dependent join pushdown for VALUES")
            }
            other => not_implemented!("dependent join pushdown for node: {other:?}"),
        }
    }

    fn pushdown_children(
        &mut self,
        bind_context: &mut BindContext,
        children: &mut [LogicalOperator],
    ) -> Result<()> {
        for child in children {
            self.pushdown(bind_context, child)?;
        }
        Ok(())
    }

    // TODO: Should accept logical node trait.
    fn any_expression_has_correlation<'a>(
        &self,
        exprs: impl IntoIterator<Item = &'a Expression>,
    ) -> bool {
        exprs
            .into_iter()
            .any(|e| self.expression_has_correlation(e))
    }

    fn expression_has_correlation(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Column(col) => self
                .columns
                .iter()
                .any(|c| c.table == col.reference.table_scope && c.col_idx == col.reference.column),
            other => {
                let mut has_correlation = false;
                other
                    .for_each_child(&mut |child| {
                        if has_correlation {
                            return Ok(());
                        }
                        has_correlation = self.expression_has_correlation(child);
                        Ok(())
                    })
                    .expect("expr correlation walk to not fail");
                has_correlation
            }
        }
    }

    fn rewrite_expressions(
        &self,
        bind_context: &BindContext,
        exprs: &mut [Expression],
    ) -> Result<()> {
        for expr in exprs {
            self.rewrite_expression(bind_context, expr)?;
        }
        Ok(())
    }

    fn rewrite_expression(&self, bind_context: &BindContext, expr: &mut Expression) -> Result<()> {
        match expr {
            Expression::Column(col) => {
                if let Some(correlated) = self.columns.iter().find(|corr| {
                    corr.table == col.reference.table_scope && corr.col_idx == col.reference.column
                }) {
                    // Correlated column found, update to mapped column.
                    let new_col = *self.column_map.get(correlated).ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing correlated column in column map: {correlated:?}"
                        ))
                    })?;

                    let datatype = bind_context.get_column_type(new_col)?;

                    *expr = Expression::Column(ColumnExpr {
                        reference: new_col,
                        datatype,
                    });
                }

                // Column we're not concerned about. Remains unchanged.
                Ok(())
            }
            other => {
                other.for_each_child_mut(&mut |child| self.rewrite_expression(bind_context, child))
            }
        }
    }
}
