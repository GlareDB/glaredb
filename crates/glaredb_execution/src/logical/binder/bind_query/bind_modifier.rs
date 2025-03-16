use std::fmt;

use glaredb_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::select_list::SelectList;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::column_binder::{DefaultColumnBinder, ExpressionColumnBinder};
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundOrderByExpr {
    pub expr: Expression,
    pub desc: bool,
    pub nulls_first: bool,
}

impl fmt::Display for BoundOrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.desc { "desc" } else { "asc" },
            if self.nulls_first {
                "nulls_first"
            } else {
                "nulls_last"
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOrderBy {
    pub exprs: Vec<BoundOrderByExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundLimit {
    pub limit: usize,
    pub offset: Option<usize>,
}

/// Binds ORDER BY, LIMIT, and DISTINCT.
#[derive(Debug)]
pub struct ModifierBinder<'a> {
    /// Contexts in scope.
    ///
    /// Should be a length of 1 for typical select query, and length or two for
    /// set operations.
    pub current: Vec<BindScopeRef>,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ModifierBinder<'a> {
    pub fn new(current: Vec<BindScopeRef>, resolve_context: &'a ResolveContext) -> Self {
        ModifierBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_order_by(
        &self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        order_by: ast::OrderByModifier<ResolvedMeta>,
    ) -> Result<BoundOrderBy> {
        // TODO: What do here?
        let current = match self.current.first() {
            Some(&current) => current,
            None => return Err(RayexecError::new("Missing scope, cannot bind to anything")),
        };

        let exprs = order_by
            .order_by_nodes
            .into_iter()
            .map(|order_by| {
                let mut column_binder = OrderByColumnBinder {
                    select_list,
                    did_bind_to_select: false,
                };

                let expr = BaseExpressionBinder::new(current, self.resolve_context)
                    .bind_expression(
                        bind_context,
                        &order_by.expr,
                        &mut column_binder,
                        RecursionContext {
                            allow_windows: false,
                            allow_aggregates: false,
                            is_root: true,
                        },
                    )?;

                // If we bound to an existing item in the select list, use that
                // expression. If we didn't, push the expression to the appended
                // list, and bind to to that instead.
                let expr = if column_binder.did_bind_to_select {
                    expr
                } else {
                    let col = select_list.append_projection(bind_context, expr)?;
                    Expression::Column(col)
                };

                // ASC is default.
                let desc = matches!(
                    order_by.typ.unwrap_or(ast::OrderByType::Asc),
                    ast::OrderByType::Desc
                );

                // Nulls ordered as if larger than any other value (to match
                // postgres).
                //
                // ASC => NULLS LAST
                // DESC => NULLS FIRST
                let nulls_first = match order_by.nulls {
                    Some(nulls) => matches!(nulls, ast::OrderByNulls::First),
                    None => desc,
                };

                Ok(BoundOrderByExpr {
                    expr,
                    desc,
                    nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(BoundOrderBy { exprs })
    }

    pub fn bind_limit(
        &self,
        bind_context: &mut BindContext,
        limit_mod: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<Option<BoundLimit>> {
        // TODO: What do here?
        let current = match self.current.first() {
            Some(&current) => current,
            None => return Err(RayexecError::new("Missing scope, cannot bind to anything")),
        };

        let expr_binder = BaseExpressionBinder::new(current, self.resolve_context);

        let limit = match limit_mod.limit {
            Some(limit) => expr_binder.bind_expression(
                bind_context,
                &limit,
                &mut DefaultColumnBinder,
                RecursionContext {
                    allow_windows: false,
                    allow_aggregates: false,
                    is_root: true,
                },
            )?,
            None => {
                if limit_mod.offset.is_some() {
                    return Err(RayexecError::new("OFFSET without LIMIT not supported"));
                }
                return Ok(None);
            }
        };

        let limit = limit.try_into_scalar()?.try_as_i64()?;
        let limit = if limit < 0 {
            return Err(RayexecError::new("LIMIT cannot be negative"));
        } else {
            limit as usize
        };

        let offset = match limit_mod.offset {
            Some(offset) => {
                let offset = expr_binder.bind_expression(
                    bind_context,
                    &offset,
                    &mut DefaultColumnBinder,
                    RecursionContext {
                        allow_windows: false,
                        allow_aggregates: false,
                        is_root: true,
                    },
                )?;
                let offset = offset.try_into_scalar()?.try_as_i64()?;
                if offset < 0 {
                    return Err(RayexecError::new("OFFSET cannot be negative"));
                } else {
                    Some(offset as usize)
                }
            }
            None => None,
        };

        Ok(Some(BoundLimit { limit, offset }))
    }
}

#[derive(Debug)]
pub struct OrderByColumnBinder<'a> {
    select_list: &'a SelectList,
    did_bind_to_select: bool,
}

impl ExpressionColumnBinder for OrderByColumnBinder<'_> {
    fn bind_from_root_literal(
        &mut self,
        _bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>> {
        if let Some(col) = self.select_list.column_by_ordinal(bind_context, literal)? {
            self.did_bind_to_select = true;
            return Ok(Some(Expression::Column(col)));
        }
        Ok(None)
    }

    fn bind_from_ident(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        ident: &ast::Ident,
        recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        // Try to bind normally.
        if let Some(col) =
            DefaultColumnBinder.bind_from_ident(bind_scope, bind_context, ident, recur)?
        {
            return Ok(Some(col));
        }

        // Only allow binding alias if we at the root of the expressions.
        if !recur.is_root {
            return Ok(None);
        }

        // Try to bind to a user-provided alias.
        if let Some(col) = self.select_list.column_by_user_alias(bind_context, ident)? {
            self.did_bind_to_select = true;
            return Ok(Some(Expression::Column(col)));
        }

        Ok(None)
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
