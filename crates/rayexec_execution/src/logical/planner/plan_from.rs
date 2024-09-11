use rayexec_bullet::scalar::ScalarValue;
use rayexec_error::{not_implemented, RayexecError, Result};

use crate::{
    expr::{
        column_expr::ColumnExpr, comparison_expr::ComparisonExpr, literal_expr::LiteralExpr,
        Expression,
    },
    logical::{
        binder::{
            bind_context::BindContext,
            bind_query::{
                bind_from::{BoundFrom, BoundFromItem, BoundJoin},
                condition_extractor::JoinConditionExtractor,
            },
        },
        logical_empty::LogicalEmpty,
        logical_filter::LogicalFilter,
        logical_join::{JoinType, LogicalArbitraryJoin, LogicalComparisonJoin, LogicalCrossJoin},
        logical_materialization::LogicalMaterializationScan,
        logical_project::LogicalProject,
        logical_scan::{LogicalScan, ScanSource},
        operator::{LocationRequirement, LogicalNode, LogicalOperator, Node},
    },
};

use super::plan_query::QueryPlanner;

#[derive(Debug)]
pub struct FromPlanner;

impl FromPlanner {
    pub fn plan(&self, bind_context: &mut BindContext, from: BoundFrom) -> Result<LogicalOperator> {
        match from.item {
            BoundFromItem::BaseTable(table) => {
                let mut types = Vec::new();
                let mut names = Vec::new();
                for table in bind_context.iter_tables(from.bind_ref)? {
                    types.extend(table.column_types.iter().cloned());
                    names.extend(table.column_names.iter().cloned());
                }

                let projection = (0..types.len()).collect();

                Ok(LogicalOperator::Scan(Node {
                    node: LogicalScan {
                        table_ref: table.table_ref,
                        types,
                        names,
                        projection,
                        source: ScanSource::Table {
                            catalog: table.catalog,
                            schema: table.schema,
                            source: table.entry,
                        },
                    },
                    location: table.location,
                    children: Vec::new(),
                }))
            }
            BoundFromItem::Join(join) => self.plan_join(bind_context, join),
            BoundFromItem::TableFunction(func) => {
                let mut types = Vec::new();
                let mut names = Vec::new();
                for table in bind_context.iter_tables(from.bind_ref)? {
                    types.extend(table.column_types.iter().cloned());
                    names.extend(table.column_names.iter().cloned());
                }

                let projection = (0..types.len()).collect();

                Ok(LogicalOperator::Scan(Node {
                    node: LogicalScan {
                        table_ref: func.table_ref,
                        types,
                        names,
                        projection,
                        source: ScanSource::TableFunction {
                            function: func.function,
                        },
                    },
                    location: func.location,
                    children: Vec::new(),
                }))
            }
            BoundFromItem::Subquery(subquery) => {
                let plan = QueryPlanner.plan(bind_context, *subquery.subquery)?;

                // Project subquery columns into this scope.
                //
                // The binding scope for a subquery is nested relative to a
                // parent scope, so this project lets us resolve all columns
                // without special-casing from binding.
                let mut projections = Vec::new();
                for table_ref in plan.get_output_table_refs() {
                    let table = bind_context.get_table(table_ref)?;
                    for col_idx in 0..table.num_columns() {
                        projections.push(Expression::Column(ColumnExpr {
                            table_scope: table_ref,
                            column: col_idx,
                        }));
                    }
                }

                Ok(LogicalOperator::Project(Node {
                    node: LogicalProject {
                        projections,
                        projection_table: subquery.table_ref,
                    },
                    location: LocationRequirement::Any,
                    children: vec![plan],
                }))
            }
            BoundFromItem::MaterializedCte(mat_cte) => {
                // TODO: Have a way of indexing to the CTE directly instead of
                // searching the scopes again.
                let cte = bind_context.get_cte(mat_cte.cte_ref)?;

                let mat_ref = match cte.mat_ref {
                    Some(mat_ref) => {
                        // Already have materialization, increment the scan
                        // count.
                        bind_context.inc_materialization_scan_count(mat_ref, 1)?;

                        mat_ref
                    }
                    None => {
                        // First time planning this CTE, go ahead and create the
                        // plan for materialization.
                        let plan = QueryPlanner.plan(bind_context, *cte.bound.clone())?;
                        let mat_ref = bind_context.new_materialization(plan)?;
                        bind_context.inc_materialization_scan_count(mat_ref, 1)?;

                        // Update the cte to now have the materialized reference.
                        let cte = bind_context.get_cte_mut(mat_cte.cte_ref)?;
                        cte.mat_ref = Some(mat_ref);

                        mat_ref
                    }
                };

                let mat = bind_context.get_materialization(mat_ref)?;

                // Similarly to subqueries, we add a project here to ensure all
                // columns from the CTE an brought into the current scope.
                let mut projections = Vec::new();
                for table_ref in mat.plan.get_output_table_refs() {
                    let table = bind_context.get_table(table_ref)?;
                    for col_idx in 0..table.num_columns() {
                        projections.push(Expression::Column(ColumnExpr {
                            table_scope: table_ref,
                            column: col_idx,
                        }));
                    }
                }

                Ok(LogicalOperator::Project(Node {
                    node: LogicalProject {
                        projections,
                        projection_table: mat_cte.table_ref,
                    },
                    location: LocationRequirement::Any,
                    children: vec![LogicalOperator::MaterializationScan(Node {
                        node: LogicalMaterializationScan {
                            mat: mat.mat_ref,
                            table_ref: mat.table_ref,
                        },
                        location: LocationRequirement::Any,
                        children: Vec::new(),
                    })],
                }))
            }
            BoundFromItem::Empty => Ok(LogicalOperator::Empty(Node {
                node: LogicalEmpty,
                location: LocationRequirement::Any,
                children: Vec::new(),
            })),
        }
    }

    fn plan_join(
        &self,
        bind_context: &mut BindContext,
        join: BoundJoin,
    ) -> Result<LogicalOperator> {
        if join.lateral {
            not_implemented!("LATERAL join")
        }

        let mut left = self.plan(bind_context, *join.left)?;
        let mut right = self.plan(bind_context, *join.right)?;

        // Cross join.
        if join.conditions.is_empty() {
            if !join.conditions.is_empty() {
                return Err(RayexecError::new("CROSS JOIN should not have conditions"));
            }
            return Ok(LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![left, right],
            }));
        }

        let extractor = JoinConditionExtractor::new(
            bind_context,
            join.left_bind_ref,
            join.right_bind_ref,
            join.join_type,
        );

        let extracted = extractor.extract(join.conditions)?;

        if !extracted.left_filter.is_empty() {
            left = LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: Expression::and_all(extracted.left_filter)
                        .expect("at least one expression"),
                },
                location: LocationRequirement::Any,
                children: vec![left],
            })
        }

        if !extracted.right_filter.is_empty() {
            right = LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: Expression::and_all(extracted.right_filter)
                        .expect("at least one expression"),
                },
                location: LocationRequirement::Any,
                children: vec![right],
            })
        }

        // Need to use an arbitrary join if:
        //
        // - We didn't extract any comparison conditions.
        // - We're not an INNER join and we have arbitrary expressions.
        let use_arbitrary_join = extracted.comparisons.is_empty()
            || (join.join_type != JoinType::Inner && !extracted.arbitrary.is_empty());

        if use_arbitrary_join {
            let mut expressions = extracted.arbitrary;
            for condition in extracted.comparisons {
                expressions.push(Expression::Comparison(ComparisonExpr {
                    left: Box::new(condition.left),
                    right: Box::new(condition.right),
                    op: condition.op,
                }));
            }

            // Possible if we were able to push filters to left/right
            // completely.
            if expressions.is_empty() {
                expressions.push(Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(true),
                }));
            }

            return Ok(LogicalOperator::ArbitraryJoin(Node {
                node: LogicalArbitraryJoin {
                    join_type: join.join_type,
                    condition: Expression::and_all(expressions).expect("at least one expression"),
                },
                location: LocationRequirement::Any,
                children: vec![left, right],
            }));
        }

        // Otherwise we're able to use a comparison join.
        let mut plan = LogicalOperator::ComparisonJoin(Node {
            node: LogicalComparisonJoin {
                join_type: join.join_type,
                conditions: extracted.comparisons,
            },
            location: LocationRequirement::Any,
            children: vec![left, right],
        });

        // Push filter if we have arbitrary expressions.
        if !extracted.arbitrary.is_empty() {
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: Expression::and_all(extracted.arbitrary)
                        .expect("at least one expression"),
                },
                location: LocationRequirement::Any,
                children: vec![plan],
            })
        }

        Ok(plan)
    }
}
