use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast;

use super::{BoundQuery, QueryBinder};
use crate::arrays::datatype::DataType;
use crate::database::catalog_entry::CatalogEntry;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::Expression;
use crate::functions::table::{PlannedTableFunction, TableFunctionPlanner};
use crate::logical::binder::bind_context::{
    BindContext,
    BindScopeRef,
    CorrelatedColumn,
    CteRef,
    UsingColumn,
};
use crate::logical::binder::column_binder::DefaultColumnBinder;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::binder::table_list::{TableAlias, TableRef};
use crate::logical::logical_join::JoinType;
use crate::logical::operator::LocationRequirement;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::resolved_table::ResolvedTableOrCteReference;
use crate::logical::resolver::resolved_table_function::ResolvedTableFunctionReference;
use crate::logical::resolver::{ResolvedMeta, ResolvedSubqueryOptions};
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundFrom {
    pub bind_ref: BindScopeRef,
    pub item: BoundFromItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundFromItem {
    BaseTable(BoundBaseTable),
    Join(BoundJoin),
    TableFunction(BoundTableFunction),
    Subquery(BoundSubquery),
    MaterializedCte(BoundMaterializedCte),
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundBaseTable {
    pub table_ref: TableRef,
    pub location: LocationRequirement,
    pub catalog: String,
    pub schema: String,
    pub entry: Arc<CatalogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundTableFunction {
    pub table_ref: TableRef,
    pub location: LocationRequirement,
    pub function: PlannedTableFunction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSubquery {
    pub table_ref: TableRef,
    pub subquery: Box<BoundQuery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundMaterializedCte {
    pub table_ref: TableRef,
    pub cte_ref: CteRef,
    pub cte_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundJoin {
    /// Reference to binder for left side of join.
    pub left_bind_ref: BindScopeRef,
    /// Bound left.
    pub left: Box<BoundFrom>,
    /// Reference to binder for right side of join.
    pub right_bind_ref: BindScopeRef,
    /// Bound right.
    pub right: Box<BoundFrom>,
    /// Join type.
    pub join_type: JoinType,
    /// Expressions we're joining on, if any.
    pub conditions: Vec<Expression>,
    /// Columns from the left side of the join being referenced on the right.
    pub lateral_columns: Vec<CorrelatedColumn>,
}

#[derive(Debug)]
pub struct FromBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> FromBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        FromBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        from: Option<ast::FromNode<ResolvedMeta>>,
    ) -> Result<BoundFrom> {
        let from = match from {
            Some(from) => from,
            None => {
                return Ok(BoundFrom {
                    bind_ref: self.current,
                    item: BoundFromItem::Empty,
                })
            }
        };

        match from.body {
            ast::FromNodeBody::BaseTable(table) => self.bind_table(bind_context, table, from.alias),
            ast::FromNodeBody::Join(join) => self.bind_join(bind_context, join), // TODO: What to do with alias?
            ast::FromNodeBody::TableFunction(func) => {
                self.bind_function(bind_context, func, from.alias)
            }
            ast::FromNodeBody::Subquery(subquery) => {
                self.bind_subquery(bind_context, subquery, from.alias)
            }
            ast::FromNodeBody::File(_) => Err(RayexecError::new(
                "Resolver should have replaced file path with a table function",
            )),
        }
    }

    fn push_table_scope_with_from_alias(
        &self,
        bind_context: &mut BindContext,
        mut default_alias: Option<TableAlias>,
        mut default_column_aliases: Vec<String>,
        column_types: Vec<DataType>,
        from_alias: Option<ast::FromAlias>,
    ) -> Result<TableRef> {
        match from_alias {
            Some(ast::FromAlias { alias, columns }) => {
                default_alias = Some(TableAlias {
                    database: None,
                    schema: None,
                    table: alias.into_normalized_string(),
                });

                // If column aliases are provided as well, apply those to the
                // columns in the scope.
                //
                // Note that if the user supplies less aliases than there are
                // columns in the scope, then the remaining columns will retain
                // their original names.
                if let Some(columns) = columns {
                    if columns.len() > default_column_aliases.len() {
                        return Err(RayexecError::new(format!(
                            "Specified {} column aliases when only {} columns exist",
                            columns.len(),
                            default_column_aliases.len(),
                        )));
                    }

                    for (orig_alias, new_alias) in
                        default_column_aliases.iter_mut().zip(columns.into_iter())
                    {
                        *orig_alias = new_alias.into_normalized_string();
                    }
                }
            }
            None => {
                // Keep aliases unmodified.
            }
        }

        bind_context.push_table(
            self.current,
            default_alias,
            column_types,
            default_column_aliases,
        )
    }

    pub(crate) fn bind_table(
        &self,
        bind_context: &mut BindContext,
        table: ast::FromBaseTable<ResolvedMeta>,
        alias: Option<ast::FromAlias>,
    ) -> Result<BoundFrom> {
        match self.resolve_context.tables.try_get_bound(table.reference)? {
            (ResolvedTableOrCteReference::Table(table), location) => {
                let column_types = table
                    .entry
                    .try_as_table_entry()?
                    .columns
                    .iter()
                    .map(|c| c.datatype.clone())
                    .collect();
                let column_names = table
                    .entry
                    .try_as_table_entry()?
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let default_alias = TableAlias {
                    database: Some(table.catalog.clone()),
                    schema: Some(table.schema.clone()),
                    table: table.entry.name.clone(),
                };

                let table_ref = self.push_table_scope_with_from_alias(
                    bind_context,
                    Some(default_alias),
                    column_names,
                    column_types,
                    alias,
                )?;

                Ok(BoundFrom {
                    bind_ref: self.current,
                    item: BoundFromItem::BaseTable(BoundBaseTable {
                        table_ref,
                        location,
                        catalog: table.catalog.clone(),
                        schema: table.schema.clone(),
                        entry: table.entry.clone(),
                    }),
                })
            }
            (ResolvedTableOrCteReference::Cte(name), _location) => {
                // TODO: Does location matter here?
                self.bind_cte(bind_context, name, alias)
            }
        }
    }

    fn bind_cte(
        &self,
        bind_context: &mut BindContext,
        cte: &str,
        alias: Option<ast::FromAlias>,
    ) -> Result<BoundFrom> {
        let cte_ref = bind_context.find_cte(self.current, cte)?;
        let cte = bind_context.get_cte(cte_ref)?;

        let table_alias = TableAlias {
            database: None,
            schema: None,
            table: cte.name.clone(),
        };

        let names = cte.column_names.clone();
        let types = cte.column_types.clone();

        if cte.materialized {
            let cte_name = cte.name.clone();
            // Binds with the alias provided in the FROM.
            //
            // ... FROM mycte AS aliased_cte(c1, c2) ...
            let table_ref = self.push_table_scope_with_from_alias(
                bind_context,
                Some(table_alias),
                names,
                types,
                alias,
            )?;

            Ok(BoundFrom {
                bind_ref: self.current,
                item: BoundFromItem::MaterializedCte(BoundMaterializedCte {
                    table_ref,
                    cte_ref,
                    cte_name,
                }),
            })
        } else {
            // Not materialize, just copy the plan as a subquery.
            let subquery = cte.bound.clone();

            // Binds with the alias provided in the FROM.
            //
            // ... FROM mycte AS aliased_cte(c1, c2) ...
            let table_ref = self.push_table_scope_with_from_alias(
                bind_context,
                Some(table_alias),
                names,
                types,
                alias,
            )?;

            Ok(BoundFrom {
                bind_ref: self.current,
                item: BoundFromItem::Subquery(BoundSubquery {
                    table_ref,
                    subquery,
                }),
            })
        }
    }

    fn bind_subquery(
        &self,
        bind_context: &mut BindContext,
        subquery: ast::FromSubquery<ResolvedMeta>,
        alias: Option<ast::FromAlias>,
    ) -> Result<BoundFrom> {
        // We can automatically detect lateral joins, doesn't matter what this
        // is.
        let _ = subquery.lateral;

        let nested_scope = match &subquery.options {
            ResolvedSubqueryOptions::Normal => bind_context.new_child_scope(self.current),
            ResolvedSubqueryOptions::View { .. } => bind_context.new_orphan_scope(), // View can only reference itself.
        };
        let binder = QueryBinder::new(nested_scope, self.resolve_context);

        let bound = binder.bind(bind_context, subquery.query)?;

        let mut names = Vec::new();
        let mut types = Vec::new();
        for table in bind_context.iter_tables_in_scope(nested_scope)? {
            types.extend(table.column_types.iter().cloned());
            names.extend(table.column_names.iter().cloned());
        }

        let table_ref = if let ResolvedSubqueryOptions::View {
            table_alias,
            column_aliases,
        } = subquery.options
        {
            // If we're a view, ensure we apply the right column aliases _and_
            // include a table alias when creating a ref in the bind context
            // (for qualified column references).
            //
            // Columns aliases defined in the view may be overridden in
            // `push_table_scope_with_from_alias`. This just sets the default
            // names.
            if column_aliases.len() > names.len() {
                return Err(RayexecError::new(format!(
                    "View contains too many column aliases, expected {}, got {}",
                    names.len(),
                    column_aliases.len()
                )));
            }

            for (name, alias) in names.iter_mut().zip(column_aliases) {
                *name = alias;
            }

            self.push_table_scope_with_from_alias(
                bind_context,
                Some(table_alias),
                names,
                types,
                alias,
            )?
        } else {
            // Nothing special, just a normal subquery so no table alias.
            self.push_table_scope_with_from_alias(bind_context, None, names, types, alias)?
        };

        // Move correlated columns into current scope.
        bind_context.append_correlated_columns(self.current, nested_scope)?;

        Ok(BoundFrom {
            bind_ref: self.current,
            item: BoundFromItem::Subquery(BoundSubquery {
                table_ref,
                subquery: Box::new(bound),
            }),
        })
    }

    fn bind_function(
        &self,
        bind_context: &mut BindContext,
        function: ast::FromTableFunction<ResolvedMeta>,
        alias: Option<ast::FromAlias>,
    ) -> Result<BoundFrom> {
        // See above, lateral joins automatically detected.
        let _ = function.lateral;

        let (reference, location) = self
            .resolve_context
            .table_functions
            .try_get_bound(function.reference)?;

        let planned = match reference {
            ResolvedTableFunctionReference::InOut(inout) => {
                // Handle in/out function planning now. We have everything we
                // need to plan its inputs.
                let expr_binder = BaseExpressionBinder::new(self.current, self.resolve_context);

                let mut positional = Vec::new();
                let mut named = HashMap::new();

                for arg in function.args.iter() {
                    let recur = RecursionContext {
                        allow_aggregates: false,
                        allow_windows: false,
                        is_root: true,
                    };

                    match arg {
                        ast::FunctionArg::Unnamed { arg } => match arg {
                            ast::FunctionArgExpr::Expr(expr) => {
                                let expr = expr_binder.bind_expression(
                                    bind_context,
                                    expr,
                                    &mut DefaultColumnBinder,
                                    recur,
                                )?;

                                positional.push(expr);
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                return Err(RayexecError::new(
                                    "Cannot plan a function with '*' as an argument",
                                ));
                            }
                        },
                        ast::FunctionArg::Named { name, arg } => {
                            match arg {
                                ast::FunctionArgExpr::Expr(expr) => {
                                    // Constants required.
                                    let expr = expr_binder.bind_expression(
                                        bind_context,
                                        expr,
                                        &mut DefaultColumnBinder,
                                        recur,
                                    )?;

                                    let val =
                                        ConstFold::rewrite(bind_context.get_table_list(), expr)?
                                            .try_into_scalar()?;
                                    named.insert(name.as_normalized_string(), val);
                                }
                                ast::FunctionArgExpr::Wildcard => {
                                    return Err(RayexecError::new(
                                        "Cannot plan a function with '*' as an argument",
                                    ));
                                }
                            }
                        }
                    }
                }

                // Note only positional input casts for now. Signatures don't
                // have a notion of named arguments yet.
                let positional = expr_binder.apply_casts_for_table_function(
                    bind_context,
                    inout.as_ref(),
                    positional,
                )?;

                match inout.planner() {
                    TableFunctionPlanner::InOut(planner) => {
                        planner.plan(bind_context.get_table_list(), positional, named)?
                    }
                    TableFunctionPlanner::Scan(_) => {
                        return Err(RayexecError::new(
                            "Expected in/out planner, got scan planner",
                        ))
                    }
                }
            }
            ResolvedTableFunctionReference::Scan(planned) => planned.clone(),
        };

        // TODO: For table funcs that are reading files, it'd be nice to have
        // the default alias be the base file path, not the function name.
        let default_alias = TableAlias {
            database: None,
            schema: None,
            table: reference.base_table_alias(),
        };

        let (names, types) = planned
            .schema
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.datatype.clone()))
            .unzip();

        let table_ref = self.push_table_scope_with_from_alias(
            bind_context,
            Some(default_alias),
            names,
            types,
            alias,
        )?;

        Ok(BoundFrom {
            bind_ref: self.current,
            item: BoundFromItem::TableFunction(BoundTableFunction {
                table_ref,
                location,
                function: planned,
            }),
        })
    }

    fn bind_join(
        &self,
        bind_context: &mut BindContext,
        join: ast::FromJoin<ResolvedMeta>,
    ) -> Result<BoundFrom> {
        // Bind left first.
        let left_idx = bind_context.new_child_scope(self.current);
        let left =
            FromBinder::new(left_idx, self.resolve_context).bind(bind_context, Some(*join.left))?;

        // Bind right.
        //
        // The right bind context is created as a child of the left bind context
        // to easily check if this is a lateral join. All we need to do is check
        // if there's any correlated columns that come out of the bind that
        // match the left bind scope.
        let right_idx = bind_context.new_child_scope(left_idx);
        let right = FromBinder::new(right_idx, self.resolve_context)
            .bind(bind_context, Some(*join.right))?;

        // Lateral columns are columns from the right the reference the output
        // of the left.
        //
        // These will be passed to the subquery planner for flattening.
        let lateral_columns: Vec<_> = bind_context
            .correlated_columns(right_idx)?
            .iter()
            .filter(|c| c.outer == left_idx)
            .cloned()
            .collect();

        let (conditions, using_cols) = match join.join_condition {
            ast::JoinCondition::On(exprs) => (vec![exprs], Vec::new()),
            ast::JoinCondition::Using(cols) => {
                let using_cols: Vec<_> = cols
                    .into_iter()
                    .map(|c| c.into_normalized_string())
                    .collect();
                (Vec::new(), using_cols)
            }
            ast::JoinCondition::Natural => {
                // Get tables refs from the left.
                //
                // We want to prune these tables out from the right. Tables are
                // implicitly in scope on the right for lateral references.
                let left_tables: HashSet<_> = bind_context
                    .iter_tables_in_scope(left_idx)?
                    .map(|table| table.reference)
                    .collect();

                // Get columns from the left.
                let left_cols: HashSet<_> = bind_context
                    .iter_tables_in_scope(left_idx)?
                    .flat_map(|table| table.column_names.iter())
                    .collect();

                // Get columns from the right, skipping columns from tables that
                // would generate a lateral reference.
                let right_cols = bind_context
                    .iter_tables_in_scope(right_idx)?
                    .filter(|table| !left_tables.contains(&table.reference))
                    .flat_map(|table| table.column_names.iter());

                let mut common = Vec::new();

                // Now collect the columns that are common in both.
                //
                // Manually iterate over using a hash set intersection to keep
                // the order of columns consistent.
                for right_col in right_cols {
                    if left_cols.contains(right_col) {
                        common.push(right_col.clone());
                    }
                }

                (Vec::new(), common)
            }
            ast::JoinCondition::None => (Vec::new(), Vec::new()),
        };

        let join_type = match join.join_type {
            ast::JoinType::Inner => JoinType::Inner,
            ast::JoinType::Left => JoinType::Left,
            ast::JoinType::Right => JoinType::Right,
            ast::JoinType::LeftSemi => JoinType::LeftSemi,
            other => not_implemented!("plan join type: {other:?}"),
        };

        // Move left and right into current context.
        bind_context.append_context(self.current, left_idx)?;
        bind_context.append_context(self.current, right_idx)?;

        let condition_binder = BaseExpressionBinder::new(self.current, self.resolve_context);
        let mut conditions = condition_binder.bind_expressions(
            bind_context,
            &conditions,
            &mut DefaultColumnBinder,
            RecursionContext {
                allow_windows: false,
                allow_aggregates: false,
                is_root: true,
            },
        )?;

        // Handle any USING columns, adding conditions as needed.
        for using in using_cols {
            let missing_column = |side| {
                RayexecError::new(format!(
                    "Cannot find column '{using}' on {side} side of join"
                ))
            };

            let (left_table, left_col_idx) = bind_context
                .find_table_for_column(left_idx, None, &using)?
                .ok_or_else(|| missing_column("left"))?;
            let (right_table, right_col_idx) = bind_context
                .find_table_for_column(right_idx, None, &using)?
                .ok_or_else(|| missing_column("right"))?;

            let using_column = match join_type {
                JoinType::Left
                | JoinType::Inner
                | JoinType::Full
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark { .. } => UsingColumn {
                    column: using,
                    table_ref: left_table,
                    col_idx: left_col_idx,
                },
                JoinType::Right => UsingColumn {
                    column: using,
                    table_ref: right_table,
                    col_idx: right_col_idx,
                },
            };

            // Add USING column to _current_ scope if we don't already have an
            // equivalent column in our using set.
            let already_using = bind_context
                .get_using_columns(self.current)?
                .iter()
                .any(|c| c.column == using_column.column);

            if !already_using {
                bind_context.append_using_column(self.current, using_column)?;
            }

            // Generate additional equality condition.
            // TODO: Probably make this a method on the expr binder. Easy to miss the cast.
            let [left, right] = condition_binder.apply_cast_for_operator(
                bind_context,
                ComparisonOperator::Eq,
                [
                    Expression::Column(ColumnExpr {
                        table_scope: left_table,
                        column: left_col_idx,
                    }),
                    Expression::Column(ColumnExpr {
                        table_scope: right_table,
                        column: right_col_idx,
                    }),
                ],
            )?;

            conditions.push(Expression::Comparison(ComparisonExpr {
                left: Box::new(left),
                right: Box::new(right),
                op: ComparisonOperator::Eq,
            }))
        }

        // Remove right columns from scope for semi joins.
        if join_type == JoinType::LeftSemi {
            let right_tables: Vec<_> = bind_context
                .iter_tables_in_scope(right_idx)?
                .map(|t| t.reference)
                .collect();
            bind_context.remove_tables(self.current, &right_tables)?;
        }

        Ok(BoundFrom {
            bind_ref: self.current,
            item: BoundFromItem::Join(BoundJoin {
                left_bind_ref: left_idx,
                left: Box::new(left),
                right_bind_ref: right_idx,
                right: Box::new(right),
                join_type,
                conditions,
                lateral_columns,
            }),
        })
    }
}
