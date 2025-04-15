use std::collections::HashMap;

use glaredb_error::{DbError, Result, not_implemented};
use glaredb_parser::ast;

use super::select_expr_expander::ExpandedSelectExpr;
use super::select_list::{BoundDistinctModifier, SelectList};
use crate::expr::Expression;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::expr::subquery_expr::SubqueryType;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::column_binder::{DefaultColumnBinder, ExpressionColumnBinder};
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::binder::table_list::TableRef;
use crate::logical::resolver::ResolvedMeta;
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
        distinct: Option<ast::DistinctModifier<ResolvedMeta>>,
    ) -> Result<SelectList> {
        let mut alias_map = HashMap::new();

        // Track all aliases up front. This allows us to give a better error
        // if a user tries to reference an alias before it's been bound,
        //
        // E.g. we don't allow the following:
        // ```
        // SELECT a + 8, 10 AS a;
        // ```
        //
        // But we do allow:
        // ```
        // SELECT 10 AS a, a + 8
        // ```
        for (idx, projection) in projections.iter().enumerate() {
            if let Some(alias) = projection.get_alias() {
                alias_map.insert(alias.to_string(), idx);
            }
        }

        // Bind the expressions.
        let expr_binder = BaseExpressionBinder::new(self.current, self.resolve_context);
        let mut exprs = Vec::with_capacity(projections.len());
        // We compute the names based on the expression themselves. When we
        // finalize the select list, these names will be overwritten with any
        // user provide alias.
        let mut names = Vec::with_capacity(projections.len());

        for (idx, proj) in projections.into_iter().enumerate() {
            match proj {
                ExpandedSelectExpr::Expr { expr, .. } => {
                    // We pass in the alias map to allow for expressions to bind
                    // to previously bound expressions.
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
                    let mut col_binder = SelectAliasColumnBinder {
                        current_idx: idx,
                        alias_map: &alias_map,
                        previous_exprs: &exprs,
                    };

                    let bound_expr = expr_binder.bind_expression(
                        bind_context,
                        &expr,
                        &mut col_binder,
                        RecursionContext {
                            allow_windows: true,
                            allow_aggregates: true,
                            is_root: true,
                        },
                    )?;

                    let name = generate_column_name(bind_context, &bound_expr)?;

                    names.push(name);
                    exprs.push(bound_expr);
                }
                ExpandedSelectExpr::Column { expr, name } => {
                    names.push(name);
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

        // Determine distinctness of the projections.
        //
        // TODO: When ON is supported, that may include expressions not part of
        // the projection, so we'd need to add those in
        let distinct = match distinct {
            Some(ast::DistinctModifier::Distinct) => BoundDistinctModifier::Distinct,
            Some(ast::DistinctModifier::On(_)) => not_implemented!("DISTINCT ON"),
            Some(ast::DistinctModifier::All) | None => BoundDistinctModifier::All,
        };

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
            distinct_modifier: distinct,
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

/// Generates column names to use for an expression.
///
/// - Indents: Use the base identifier.
/// - Functions: Use the function name.
/// - Scalar subqueries: Use the output column name of the subquery's scope.
///
/// Expressions that we cannot (or don't want to) generate a column name for
/// will get the postgres-inspired name of "?column?".
fn generate_column_name(bind_context: &BindContext, bound_expr: &Expression) -> Result<String> {
    /// Column name to use if we can't generate one.
    const UNKNOWN_COLUMN: &str = "?column?";

    Ok(match bound_expr {
        Expression::Column(col) => {
            let (name, _) = bind_context.get_column(col.reference)?;
            name.to_string()
        }
        Expression::Aggregate(agg) => agg.agg.name.to_string(),
        Expression::ScalarFunction(func) => func.function.name.to_string(),
        Expression::Cast(cast) => generate_column_name(bind_context, &cast.expr)?,
        Expression::Subquery(subquery) if subquery.subquery_type == SubqueryType::Scalar => {
            // Use the output column names for scalar subqueries.
            let table_ref = subquery.subquery.output_table_ref();
            let (col_name, _) = bind_context.get_column((table_ref, 0))?;
            col_name.to_string()
        }
        _ => UNKNOWN_COLUMN.to_string(),
    })
}
