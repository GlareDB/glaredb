use std::collections::HashMap;

use rayexec_error::Result;
use rayexec_parser::ast;

use super::select_expr_expander::ExpandedSelectExpr;
use super::select_list::SelectList;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef, TableRef};
use crate::logical::binder::column_binder::DefaultColumnBinder;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::resolver::resolve_context::ResolveContext;

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
        for proj in projections {
            match proj {
                ExpandedSelectExpr::Expr { expr, .. } => {
                    let expr = expr_binder.bind_expression(
                        bind_context,
                        &expr,
                        &mut DefaultColumnBinder,
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
            .map(|expr| expr.datatype(bind_context))
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
                let datatype = agg.datatype(bind_context)?;
                let col_idx = bind_context.push_column_for_table(
                    aggregates_table,
                    "__generated_agg_ref",
                    datatype,
                )?;
                let agg = std::mem::replace(
                    expression,
                    Expression::Column(ColumnExpr {
                        table_scope: aggregates_table,
                        column: col_idx,
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
                    datatype,
                )?;

                let grouping = std::mem::replace(
                    expression,
                    Expression::Column(ColumnExpr {
                        table_scope: groupings_table,
                        column: col_idx,
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
            let datatype = window.datatype(bind_context)?;
            let col_idx = bind_context.push_column_for_table(
                windows_table,
                "__generated_window_ref",
                datatype,
            )?;
            let agg = std::mem::replace(
                expression,
                Expression::Column(ColumnExpr {
                    table_scope: windows_table,
                    column: col_idx,
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
