use glaredb_error::{DbError, Result};
use glaredb_parser::ast;

use super::bind_from::{BoundFrom, FromBinder};
use super::bind_group_by::{BoundGroupBy, GroupByBinder};
use super::bind_having::HavingBinder;
use super::bind_modifier::{BoundLimit, BoundOrderBy, ModifierBinder};
use super::bind_select_list::SelectListBinder;
use super::select_expr_expander::SelectExprExpander;
use super::select_list::BoundSelectList;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::column_binder::DefaultColumnBinder;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSelect {
    /// Bound projections.
    pub select_list: BoundSelectList,
    /// Bound FROM.
    pub from: BoundFrom,
    /// Expression for WHERE.
    pub filter: Option<Expression>,
    /// Expression for HAVING.
    pub having: Option<Expression>,
    /// Bound GROUP BY with expressions and grouping sets.
    pub group_by: Option<BoundGroupBy>,
    /// Bound ORDER BY.
    pub order_by: Option<BoundOrderBy>,
    /// Bound LIMIT.
    pub limit: Option<BoundLimit>,
    pub groupings: Vec<Vec<usize>>,
}

#[derive(Debug)]
pub struct SelectBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> SelectBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        SelectBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        select: ast::SelectNode<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundSelect> {
        // Handle FROM
        let from_bind_ref = bind_context.new_child_scope(self.current);
        let from =
            FromBinder::new(from_bind_ref, self.resolve_context).bind(bind_context, select.from)?;

        // Expand SELECT
        let projections = SelectExprExpander::new(from_bind_ref, bind_context)
            .expand_all_select_exprs(select.projections)?;

        if projections.is_empty() {
            return Err(DbError::new("Cannot SELECT * without a FROM clause"));
        }

        let mut select_list = SelectListBinder::new(from_bind_ref, self.resolve_context)
            .bind(bind_context, projections)?;

        // Handle WHERE
        let where_expr = select
            .where_expr
            .map(|expr| {
                let binder = BaseExpressionBinder::new(from_bind_ref, self.resolve_context);
                binder.bind_expression(
                    bind_context,
                    &expr,
                    &mut DefaultColumnBinder,
                    RecursionContext {
                        allow_windows: false,
                        allow_aggregates: false,
                        is_root: true,
                    },
                )
            })
            .transpose()?;

        // Handle ORDER BY, LIMIT, DISTINCT (todo)
        let modifier_binder = ModifierBinder::new(vec![from_bind_ref], self.resolve_context);
        let order_by = order_by
            .map(|order_by| modifier_binder.bind_order_by(bind_context, &mut select_list, order_by))
            .transpose()?;
        let limit = modifier_binder.bind_limit(bind_context, limit)?;

        // Handle GROUP BY
        let mut group_by = select
            .group_by
            .map(|group_by| {
                let mut group_by_binder = GroupByBinder::new(from_bind_ref, self.resolve_context);
                group_by_binder.bind(bind_context, &mut select_list, group_by)
            })
            .transpose()?;

        // Handle HAVING
        let mut having = select
            .having
            .map(|expr| {
                HavingBinder::new(from_bind_ref, self.resolve_context).bind(
                    bind_context,
                    &mut select_list,
                    expr,
                )
            })
            .transpose()?;

        // Finalize projections.
        let select_list = select_list.finalize(bind_context, group_by.as_mut())?;

        // Update HAVING if needed.
        if let Some(having) = &mut having {
            HavingBinder::new(from_bind_ref, self.resolve_context).update_expression_dependencies(
                &select_list,
                having,
                group_by.as_ref(),
            )?;
        }

        // Move output select columns into current scope.
        match &select_list.output {
            Some(output) => bind_context.append_table_to_scope(self.current, output.table)?,
            None => {
                bind_context.append_table_to_scope(self.current, select_list.projections_table)?
            }
        }

        // Append any correlations to current scope.
        bind_context.append_correlated_columns(self.current, from_bind_ref)?;

        Ok(BoundSelect {
            select_list,
            from,
            filter: where_expr,
            having,
            group_by,
            order_by,
            limit,
            groupings: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::logical::binder::bind_context::testutil::columns_in_scope;

    #[test]
    fn bind_context_projection_in_scope() {
        let resolve_context = ResolveContext::default();
        let mut bind_context = BindContext::new();

        let binder = SelectBinder {
            current: bind_context.root_scope_ref(),
            resolve_context: &resolve_context,
        };

        let select = ast::SelectNode {
            distinct: None,
            projections: vec![
                ast::SelectExpr::Expr(ast::Expr::Literal(ast::Literal::Number("1".to_string()))),
                ast::SelectExpr::AliasedExpr(
                    ast::Expr::Literal(ast::Literal::Number("1".to_string())),
                    ast::Ident::new_unquoted("my_alias"),
                ),
            ],
            from: None,
            where_expr: None,
            group_by: None,
            having: None,
        };

        let limit = ast::LimitModifier {
            limit: None,
            offset: None,
        };

        let _ = binder.bind(&mut bind_context, select, None, limit).unwrap();

        let cols = columns_in_scope(&bind_context, bind_context.root_scope_ref());
        let expected = vec![
            ("?column?".to_string(), DataType::Int32),
            ("my_alias".to_string(), DataType::Int32),
        ];

        assert_eq!(expected, cols);
    }
}
