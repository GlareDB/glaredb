use glaredb_error::{DbError, Result};

use super::plan_query::QueryPlanner;
use super::plan_subquery::SubqueryPlanner;
use crate::arrays::scalar::BorrowedScalarValue;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::expr::comparison_expr::ComparisonExpr;
use crate::expr::literal_expr::LiteralExpr;
use crate::expr::{self, Expression};
use crate::functions::table::TableFunctionType;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_query::bind_from::{BoundFrom, BoundFromItem, BoundJoin};
use crate::logical::logical_empty::LogicalEmpty;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_inout::LogicalTableExecute;
use crate::logical::logical_join::{
    JoinType,
    LogicalArbitraryJoin,
    LogicalComparisonJoin,
    LogicalCrossJoin,
};
use crate::logical::logical_materialization::LogicalMaterializationScan;
use crate::logical::logical_project::LogicalProject;
use crate::logical::logical_scan::{
    LogicalScan,
    ScanSource,
    TableFunctionScanSource,
    TableScanSource,
};
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::statistics::StatisticsValue;
use crate::optimizer::filter_pushdown::condition_extractor::JoinConditionExtractor;

#[derive(Debug)]
pub struct FromPlanner;

impl FromPlanner {
    pub fn plan(&self, bind_context: &mut BindContext, from: BoundFrom) -> Result<LogicalOperator> {
        match from.item {
            BoundFromItem::BaseTable(table) => {
                let mut types = Vec::new();
                let mut names = Vec::new();
                for table in bind_context.iter_tables_in_scope(from.bind_ref)? {
                    types.extend(table.column_types.iter().cloned());
                    names.extend(table.column_names.iter().cloned());
                }

                let projection = (0..types.len()).collect();

                let source = ScanSource::Table(TableScanSource {
                    catalog: table.catalog,
                    schema: table.schema,
                    source: table.entry,
                    function: table.scan_function,
                });
                let estimated_cardinality = source.cardinality();

                Ok(LogicalOperator::Scan(Node {
                    node: LogicalScan {
                        table_ref: table.table_ref,
                        types,
                        names,
                        projection,
                        scan_filters: Vec::new(),
                        source,
                    },
                    location: table.location,
                    children: Vec::new(),
                    estimated_cardinality,
                }))
            }
            BoundFromItem::Join(join) => self.plan_join(bind_context, join),
            BoundFromItem::TableFunction(func) => {
                let mut types = Vec::new();
                let mut names = Vec::new();
                for table in bind_context.iter_tables_in_scope(from.bind_ref)? {
                    types.extend(table.column_types.iter().cloned());
                    names.extend(table.column_names.iter().cloned());
                }

                match func.function.raw.function_type() {
                    TableFunctionType::Execute => {
                        let cardinality = func.function.bind_state.cardinality;

                        // In/out always requires one input. Initialize its
                        // input with an empty operator. Subquery planning will
                        // take care of the lateral binding and changing its
                        // child as needed.
                        Ok(LogicalOperator::TableExecute(Node {
                            node: LogicalTableExecute {
                                function_table_ref: func.table_ref,
                                function: func.function,
                                projected_table_ref: None,
                                projected_outputs: Vec::new(),
                            },
                            location: func.location,
                            children: vec![LogicalOperator::EMPTY],
                            estimated_cardinality: cardinality,
                        }))
                    }
                    TableFunctionType::Scan => {
                        let source = ScanSource::Function(TableFunctionScanSource {
                            function: func.function,
                        });

                        let projection = (0..types.len()).collect();
                        let estimated_cardinality = source.cardinality();

                        Ok(LogicalOperator::Scan(Node {
                            node: LogicalScan {
                                table_ref: func.table_ref,
                                types,
                                names,
                                projection,
                                scan_filters: Vec::new(),
                                source,
                            },
                            location: func.location,
                            children: Vec::new(),
                            estimated_cardinality,
                        }))
                    }
                }
            }
            BoundFromItem::Subquery(subquery) => {
                let plan = QueryPlanner.plan(bind_context, *subquery.subquery)?;

                // Project subquery columns into this scope.
                //
                // The binding scope for a subquery is nested relative to a
                // parent scope, so this project lets us resolve all columns
                // without special-casing from binding.
                let mut projections = Vec::new();
                for table_ref in plan.get_output_table_refs(bind_context) {
                    let table = bind_context.get_table(table_ref)?;
                    for col_idx in 0..table.num_columns() {
                        projections.push(Expression::Column(ColumnExpr {
                            reference: ColumnReference {
                                table_scope: table_ref,
                                column: col_idx,
                            },
                            datatype: table.column_types[col_idx].clone(),
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
                    estimated_cardinality: StatisticsValue::Unknown,
                }))
            }
            BoundFromItem::MaterializedCte(mat_cte) => {
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
                for table_ref in mat.plan.get_output_table_refs(bind_context) {
                    let table = bind_context.get_table(table_ref)?;
                    for col_idx in 0..table.num_columns() {
                        projections.push(Expression::Column(ColumnExpr {
                            reference: ColumnReference {
                                table_scope: table_ref,
                                column: col_idx,
                            },
                            datatype: table.column_types[col_idx].clone(),
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
                        node: LogicalMaterializationScan { mat: mat.mat_ref },
                        location: LocationRequirement::Any,
                        children: Vec::new(),
                        estimated_cardinality: StatisticsValue::Unknown,
                    })],
                    estimated_cardinality: StatisticsValue::Unknown,
                }))
            }
            BoundFromItem::Empty => Ok(LogicalOperator::Empty(Node {
                node: LogicalEmpty,
                location: LocationRequirement::Any,
                children: Vec::new(),
                estimated_cardinality: StatisticsValue::Unknown,
            })),
        }
    }

    fn plan_join(
        &self,
        bind_context: &mut BindContext,
        join: BoundJoin,
    ) -> Result<LogicalOperator> {
        let mut left = self.plan(bind_context, *join.left)?;
        let mut right = self.plan(bind_context, *join.right)?;

        let is_lateral = !join.lateral_columns.is_empty();

        // Cross join.
        //
        // Note that a CROSS JOIN LATERAL is implicitly an inner join, so we
        // need to keep planning that.
        if !is_lateral && join.conditions.is_empty() {
            if !join.conditions.is_empty() {
                return Err(DbError::new("CROSS JOIN should not have conditions"));
            }
            return Ok(LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![left, right],
                estimated_cardinality: StatisticsValue::Unknown,
            }));
        }

        let left_tables = left.get_output_table_refs(bind_context);
        let right_tables = right.get_output_table_refs(bind_context);

        let extractor = JoinConditionExtractor::new(&left_tables, &right_tables, join.join_type);
        let extracted = extractor.extract(join.conditions)?;

        if !extracted.left_filter.is_empty() {
            left = LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: expr::and(extracted.left_filter)?.into(),
                },
                location: LocationRequirement::Any,
                children: vec![left],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        if !extracted.right_filter.is_empty() {
            right = LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: expr::and(extracted.right_filter)?.into(),
                },
                location: LocationRequirement::Any,
                children: vec![right],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        if is_lateral {
            // Special join planning, needing to kick it to subquery planner. We
            // currently don't support arbitrary expressions for lateral join.
            if !extracted.arbitrary.is_empty() {
                return Err(DbError::new(
                    "Arbitrary expressions not yet supported for LATERAL joins",
                ));
            }

            let planned = SubqueryPlanner.plan_lateral_join(
                bind_context,
                left,
                right,
                join.join_type,
                extracted.comparisons,
                join.lateral_columns,
            )?;

            return Ok(planned);
        }

        // Normal join planning.
        self.plan_join_from_conditions(
            join.join_type,
            extracted.comparisons,
            extracted.arbitrary,
            left,
            right,
        )
    }

    pub fn plan_join_from_conditions(
        &self,
        join_type: JoinType,
        comparisons: Vec<ComparisonExpr>,
        arbitrary: Vec<Expression>,
        left: LogicalOperator,
        right: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Need to use an arbitrary join if:
        //
        // - We didn't extract any comparison conditions.
        // - We're not an INNER join and we have arbitrary expressions.
        let use_arbitrary_join =
            comparisons.is_empty() || (join_type != JoinType::Inner && !arbitrary.is_empty());

        if use_arbitrary_join {
            let mut expressions = arbitrary;
            for condition in comparisons {
                expressions.push(Expression::Comparison(condition));
            }

            // Possible if we were able to push filters to left/right
            // completely.
            if expressions.is_empty() {
                expressions.push(Expression::Literal(LiteralExpr {
                    literal: BorrowedScalarValue::Boolean(true),
                }));
            }

            return Ok(LogicalOperator::ArbitraryJoin(Node {
                node: LogicalArbitraryJoin {
                    join_type,
                    condition: expr::and(expressions)?.into(),
                },
                location: LocationRequirement::Any,
                children: vec![left, right],
                estimated_cardinality: StatisticsValue::Unknown,
            }));
        }

        // Otherwise we're able to use a comparison join.
        let mut plan = LogicalOperator::ComparisonJoin(Node {
            node: LogicalComparisonJoin {
                join_type,
                conditions: comparisons,
            },
            location: LocationRequirement::Any,
            children: vec![left, right],
            estimated_cardinality: StatisticsValue::Unknown,
        });

        // Push filter if we have arbitrary expressions.
        if !arbitrary.is_empty() {
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter {
                    filter: expr::and(arbitrary)?.into(),
                },
                location: LocationRequirement::Any,
                children: vec![plan],
                estimated_cardinality: StatisticsValue::Unknown,
            })
        }

        Ok(plan)
    }
}
