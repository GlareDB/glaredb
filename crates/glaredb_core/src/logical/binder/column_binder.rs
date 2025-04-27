use glaredb_error::{DbError, Result};
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef, CorrelatedColumn};
use super::expr_binder::RecursionContext;
use super::ident::BinderIdent;
use super::table_list::TableAlias;
use crate::expr::Expression;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::logical::resolver::ResolvedMeta;
use crate::util::fmt::displayable::IntoDisplayableSlice;

/// Defined behavior of how to bind idents to actual columns.
///
/// Implementations should typically implement custom logic first, then fall
/// back to the default binder.
pub trait ExpressionColumnBinder {
    /// Try to bind a column from a root literal.
    ///
    /// This is useful for ORDER BY and GROUP BY to bind columns by column
    /// ordinals.
    ///
    /// If Ok(None) is returned, the binder will fallback to typical literal
    /// binding to produce constant expressions.
    fn bind_from_root_literal(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>>;

    fn bind_from_ident(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        ident: &BinderIdent,
        recur: RecursionContext,
    ) -> Result<Option<Expression>>;

    fn bind_from_idents(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        idents: &[BinderIdent],
        recur: RecursionContext,
    ) -> Result<Option<Expression>>;
}

/// Default column binder.
///
/// Attempts to bind to columns in the current scope, working its way up to
/// outer scopes if the current scope does not contain a suitable columns.
///
/// Binding to an outer scope will push a correlation for the current scope.
#[derive(Debug, Clone, Copy)]
pub struct DefaultColumnBinder;

impl ExpressionColumnBinder for DefaultColumnBinder {
    fn bind_from_root_literal(
        &mut self,
        _bind_scope: BindScopeRef,
        _bind_context: &mut BindContext,
        _literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>> {
        // Binder continues with normal literal binding.
        Ok(None)
    }

    fn bind_from_ident(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        ident: &BinderIdent,
        _recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        self.bind_column(bind_scope, bind_context, None, ident)
    }

    fn bind_from_idents(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        idents: &[BinderIdent],
        _recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        let (alias, col) = idents_to_alias_and_column(idents)?;
        self.bind_column(bind_scope, bind_context, alias, &col)
    }
}

impl DefaultColumnBinder {
    /// Binds a column with the given name and optional table alias.
    ///
    /// This will handle appending correlated columns to the bind context as
    /// necessary.
    pub fn bind_column(
        &self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        alias: Option<TableAlias>,
        col: &BinderIdent,
    ) -> Result<Option<Expression>> {
        let mut current = bind_scope;
        loop {
            let table = bind_context.find_table_for_column(current, alias.as_ref(), col)?;
            match table {
                Some((table, col_idx)) => {
                    // Table containing column found. Check if it's correlated
                    // (referencing an outer context).
                    let is_correlated = current != bind_scope;

                    if is_correlated {
                        // Column is correlated, Push correlation to current
                        // bind context.
                        let correlated = CorrelatedColumn {
                            outer: current,
                            table,
                            col_idx,
                        };

                        // Note the original scope, not `current`. We want to
                        // store the context containing the expression.
                        bind_context.push_correlation(bind_scope, correlated)?;
                    }

                    let reference = ColumnReference {
                        table_scope: table,
                        column: col_idx,
                    };
                    let datatype = bind_context.get_column_type(reference)?;

                    return Ok(Some(Expression::Column(ColumnExpr {
                        reference,
                        datatype,
                    })));
                }
                None => {
                    // Table not found in current context, go to parent context
                    // relative the context we just searched.
                    match bind_context.get_parent_ref(current)? {
                        Some(parent) => current = parent,
                        None => {
                            // We're at root, no column with this ident in query.
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }
}

/// Try to convert idents into a table alias and column pair.
///
/// If only one ident is provided, table alias will be None.
///
/// Errors if no idents are provided.
fn idents_to_alias_and_column(idents: &[BinderIdent]) -> Result<(Option<TableAlias>, BinderIdent)> {
    match idents.len() {
        0 => Err(DbError::new("Empty identifier")),
        1 => {
            // Single column.
            Ok((None, idents[0].clone()))
        }
        2..=4 => {
            // Qualified column.
            // 2 => 'table.column'
            // 3 => 'schema.table.column'
            // 4 => 'database.schema.table.column'
            // TODO: Struct fields.

            let mut idents = idents.to_vec();
            let col = idents.pop().unwrap();

            let alias = TableAlias {
                table: idents.pop().unwrap(), // Must exist
                schema: idents.pop(),         // May exist
                database: idents.pop(),       // May exist
            };

            Ok((Some(alias), col))
        }
        _ => Err(DbError::new(format!(
            "Too many identifier parts in {}",
            idents.display_as_list(),
        ))), // TODO: Struct fields.
    }
}

/// Column binder that errors on any attempt to bind to a column.
#[derive(Debug, Clone, Copy)]
pub struct ErroringColumnBinder;

impl ExpressionColumnBinder for ErroringColumnBinder {
    fn bind_from_root_literal(
        &mut self,
        _bind_scope: BindScopeRef,
        _bind_context: &mut BindContext,
        _literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>> {
        // Don't error here, let the binder continue with normal literal
        // binding.
        Ok(None)
    }

    fn bind_from_ident(
        &mut self,
        _bind_scope: BindScopeRef,
        _bind_context: &mut BindContext,
        _ident: &BinderIdent,
        _recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        Err(DbError::new(
            "Statement does not support binding to columns",
        ))
    }

    fn bind_from_idents(
        &mut self,
        _bind_scope: BindScopeRef,
        _bind_context: &mut BindContext,
        _idents: &[BinderIdent],
        _recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        Err(DbError::new(
            "Statement does not support binding to columns",
        ))
    }
}
