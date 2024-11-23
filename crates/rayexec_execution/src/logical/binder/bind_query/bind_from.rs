use std::sync::Arc;

use rayexec_bullet::datatype::DataType;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast;

use super::{BoundQuery, QueryBinder};
use crate::database::catalog_entry::CatalogEntry;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::Expression;
use crate::functions::table::PlannedTableFunction;
use crate::logical::binder::bind_context::{
    BindContext,
    BindScopeRef,
    CorrelatedColumn,
    CteRef,
    TableAlias,
    TableRef,
    UsingColumn,
};
use crate::logical::binder::column_binder::DefaultColumnBinder;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::logical_join::JoinType;
use crate::logical::operator::LocationRequirement;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::resolved_table::ResolvedTableOrCteReference;
use crate::logical::resolver::{ResolvedMeta, ResolvedSubqueryOptions};

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
    pub function: Box<dyn PlannedTableFunction>,
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
    /// Columns on right side that are correlated with the left side of a join.
    pub right_correlated_columns: Vec<CorrelatedColumn>,
    /// If this is a lateral join.
    pub lateral: bool,
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
        let nested_scope = match &subquery.options {
            ResolvedSubqueryOptions::Normal => bind_context.new_child_scope(self.current),
            ResolvedSubqueryOptions::View { .. } => bind_context.new_orphan_scope(), // View can only reference itself.
        };
        let binder = QueryBinder::new(nested_scope, self.resolve_context);

        let bound = binder.bind(bind_context, subquery.query)?;

        let mut names = Vec::new();
        let mut types = Vec::new();
        for table in bind_context.iter_tables(nested_scope)? {
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
        let (reference, location) = self
            .resolve_context
            .table_functions
            .try_get_bound(function.reference)?;

        // TODO: For table funcs that are reading files, it'd be nice to have
        // the default alias be the base file path, not the function name.
        let default_alias = TableAlias {
            database: None,
            schema: None,
            table: reference.name.clone(),
        };

        let (names, types) = reference
            .func
            .schema()
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
                function: reference.func.clone(),
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
        // to easily check if this is a lateral join (distance between right and
        // left contexts == 1).
        let right_idx = bind_context.new_child_scope(left_idx);
        let right = FromBinder::new(right_idx, self.resolve_context)
            .bind(bind_context, Some(*join.right))?;

        let right_correlated_columns = bind_context.correlated_columns(right_idx)?.clone();

        // If any column in right is correlated with left, then this is a
        // lateral join.
        let any_lateral = right_correlated_columns.iter().any(|c| c.outer == left_idx);

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
                not_implemented!("NATURAL join")
            }
            ast::JoinCondition::None => (Vec::new(), Vec::new()),
        };

        let join_type = match join.join_type {
            ast::JoinType::Inner => JoinType::Inner,
            ast::JoinType::Left => JoinType::Left,
            ast::JoinType::Right => JoinType::Right,
            ast::JoinType::LeftSemi => JoinType::Semi,
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
                | JoinType::Semi
                | JoinType::Anti
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

            // Add USING column to _current_ scope.
            bind_context.append_using_column(self.current, using_column)?;

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
        if join_type == JoinType::Semi {
            let right_tables: Vec<_> = bind_context
                .iter_tables(right_idx)?
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
                right_correlated_columns,
                lateral: any_lateral,
            }),
        })
    }
}
