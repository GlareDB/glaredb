use std::collections::HashMap;

use glaredb_error::{DbError, Result};
use glaredb_parser::ast;

use super::select_expr_expander::ExpandedSelectExpr;
use super::select_list::SelectList;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::column_binder::{DefaultColumnBinder, ExpressionColumnBinder};
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::binder::table_list::TableRef;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;

#[derive(Debug)]
pub struct SelectListBinder<'a> {
    current: BindScopeRef,
    resolve_context: &'a ResolveContext,
}

impl<'a> SelectListBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        SelectListBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &mut self,
        bind_context: &mut BindContext,
        projections: Vec<ExpandedSelectExpr>,
    ) -> Result<SelectList> {
        let mut alias_map = HashMap::new();

        // Track aliases to allow referencing them in GROUP BY and ORDER BY.
        for (idx, projection) in projections.iter().enumerate() {
            if let Some(alias) = projection.get_alias() {
                alias_map.insert(alias.to_string(), idx);
            }
        }

        // Generate column names from ast expressions.
        //
        // We do this before binding the expressions in the select, as
        // projections in the select can bind to previously bound projections.
        //
        // This enables queries like `select 1 as a, a + 2`.
        //
        // This is also useful for function chaining, e.g.:
        // ```
        // SELECT
        //     string_col.upper() as u,
        //     u.contains("ABC") as c,
        //     ...
        // ```
        let mut names = projections
            .iter()
            .map(|expr| {
                Ok(match expr {
                    ExpandedSelectExpr::Expr { expr, .. } => match expr {
                        ast::Expr::Ident(ident) => ident.as_normalized_string(),
                        ast::Expr::CompoundIdent(idents) => idents
                            .last()
                            .map(|i| i.as_normalized_string())
                            .unwrap_or_else(|| "?column?".to_string()),
                        ast::Expr::Function(func) => {
                            let (func, _) = self
                                .resolve_context
                                .functions
                                .try_get_bound(func.reference)?;
                            func.name().to_string()
                        }
                        _ => "?column?".to_string(),
                    },
                    ExpandedSelectExpr::Column { name, .. } => name.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Update with user provided aliases
        //
        // TODO: This should be updated in finalize, GROUP BY may reference an
        // unaliased column.
        for (alias, idx) in &alias_map {
            names[*idx] = alias.clone();
        }

        // Bind the expressions.
        let expr_binder = BaseExpressionBinder::new(self.current, self.resolve_context);
        let mut exprs = Vec::with_capacity(projections.len());
        for (idx, proj) in projections.into_iter().enumerate() {
            match proj {
                ExpandedSelectExpr::Expr { expr, .. } => {
                    let mut col_binder = SelectAliasColumnBinder {
                        current_idx: idx,
                        alias_map: &alias_map,
                        previous_exprs: &exprs,
                    };

                    let expr = expr_binder.bind_expression(
                        bind_context,
                        &expr,
                        &mut col_binder,
                        RecursionContext {
                            allow_windows: true,
                            allow_aggregates: true,
                            is_root: true,
                        },
                    )?;
                    exprs.push(expr);
                }
                ExpandedSelectExpr::Column { expr, .. } => {
                    exprs.push(Expression::Column(expr));
                }
            }
        }

        let types = exprs
            .iter()
            .map(|expr| expr.datatype())
            .collect::<Result<Vec<_>>>()?;

        // Create table with columns. Now things can bind to the select list if
        // needed (ORDERY BY, GROUP BY).
        let projections_table = bind_context.new_ephemeral_table_with_columns(types, names)?;

        // Extract aggregates and windows into separate tables.
        let aggregates_table = bind_context.new_ephemeral_table()?;
        let groupings_table = bind_context.new_ephemeral_table()?;
        let windows_table = bind_context.new_ephemeral_table()?;

        let mut aggregates = Vec::new();
        let mut groupings = Vec::new();
        let mut windows = Vec::new();

        for expr in &mut exprs {
            Self::extract_aggregates(
                aggregates_table,
                groupings_table,
                bind_context,
                expr,
                &mut aggregates,
                &mut groupings,
            )?;
            Self::extract_windows(windows_table, bind_context, expr, &mut windows)?;
        }

        Ok(SelectList {
            projections_table,
            alias_map,
            projections: exprs,
            appended: Vec::new(),
            aggregates_table,
            aggregates,
            windows_table,
            windows,
            grouping_functions_table: groupings_table,
            grouping_set_references: groupings,
        })
    }

    /// Extracts aggregates from `expression` into `aggregates`.
    ///
    /// This will also handle extracting GROUPING calls.
    pub(crate) fn extract_aggregates(
        aggregates_table: TableRef,
        groupings_table: TableRef,
        bind_context: &mut BindContext,
        expression: &mut Expression,
        aggregates: &mut Vec<Expression>,
        groupings: &mut Vec<Expression>,
    ) -> Result<()> {
        match expression {
            Expression::Aggregate(agg) => {
                // Replace the aggregate in the projections list with a column
                // reference that points to the extracted aggregate.
                let datatype = agg.datatype()?;
                let col_idx = bind_context.push_column_for_table(
                    aggregates_table,
                    "__generated_agg_ref",
                    datatype.clone(),
                )?;
                let agg = std::mem::replace(
                    expression,
                    Expression::Column(ColumnExpr {
                        reference: ColumnReference {
                            table_scope: aggregates_table,
                            column: col_idx,
                        },
                        datatype,
                    }),
                );

                aggregates.push(agg);
                Ok(())
            }
            Expression::GroupingSet(grouping) => {
                // Similar to above, replace with column reference.
                let datatype = grouping.datatype();
                let col_idx = bind_context.push_column_for_table(
                    groupings_table,
                    "__generated_grouping_ref",
                    datatype.clone(),
                )?;

                let grouping = std::mem::replace(
                    expression,
                    Expression::Column(ColumnExpr {
                        reference: ColumnReference {
                            table_scope: groupings_table,
                            column: col_idx,
                        },
                        datatype,
                    }),
                );

                groupings.push(grouping);
                Ok(())
            }
            other => other.for_each_child_mut(&mut |expr| {
                Self::extract_aggregates(
                    aggregates_table,
                    groupings_table,
                    bind_context,
                    expr,
                    aggregates,
                    groupings,
                )
            }),
        }
    }

    /// Extracts windows from `expression` into `windows`.
    fn extract_windows(
        windows_table: TableRef,
        bind_context: &mut BindContext,
        expression: &mut Expression,
        windows: &mut Vec<Expression>,
    ) -> Result<()> {
        if let Expression::Window(window) = expression {
            // Replace the window in the projections list with a column
            // reference that points to the extracted aggregate.
            let datatype = window.datatype()?;
            let col_idx = bind_context.push_column_for_table(
                windows_table,
                "__generated_window_ref",
                datatype.clone(),
            )?;
            let agg = std::mem::replace(
                expression,
                Expression::Column(ColumnExpr {
                    reference: ColumnReference {
                        table_scope: windows_table,
                        column: col_idx,
                    },
                    datatype,
                }),
            );

            windows.push(agg);
            return Ok(());
        }

        expression.for_each_child_mut(&mut |expr| {
            Self::extract_windows(windows_table, bind_context, expr, windows)
        })?;

        Ok(())
    }
}

/// Column binder that allows binding to previously defined user aliases.
///
/// If an ident isn't found in the alias map, then default column binding is
/// used.
///
/// Aliases are only checked if normal column binding cannot find a column.
#[derive(Debug, Clone, Copy)]
struct SelectAliasColumnBinder<'a> {
    /// Index of the expression we're currently planning in the select list.
    ///
    /// Used to determine if an alias is valid to use.
    current_idx: usize,
    /// User provided aliases.
    alias_map: &'a HashMap<String, usize>,
    /// Previously planned expressions.
    previous_exprs: &'a [Expression],
}

impl ExpressionColumnBinder for SelectAliasColumnBinder<'_> {
    fn bind_from_root_literal(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>> {
        DefaultColumnBinder.bind_from_root_literal(bind_scope, bind_context, literal)
    }

    fn bind_from_ident(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        ident: &ast::Ident,
        _recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        let col = ident.as_normalized_string();

        match DefaultColumnBinder.bind_column(bind_scope, bind_context, None, &col)? {
            Some(expr) => Ok(Some(expr)),
            None => {
                match self.alias_map.get(&col) {
                    Some(&col_idx) => {
                        if col_idx < self.current_idx {
                            // Valid alias reference, use the existing expression.
                            let aliased_expr =
                                self.previous_exprs.get(col_idx).ok_or_else(|| {
                                    DbError::new("Missing select expression?")
                                        .with_field("idx", col_idx)
                                })?;

                            Ok(Some(aliased_expr.clone()))
                        } else {
                            // Not a valid alias expression.
                            Err(DbError::new(format!(
                                "'{col}' can only be referenced after it's been defined in the SELECT list"
                            )))
                        }
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn bind_from_idents(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        idents: &[ast::Ident],
        recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        DefaultColumnBinder.bind_from_idents(bind_scope, bind_context, idents, recur)
    }
}
