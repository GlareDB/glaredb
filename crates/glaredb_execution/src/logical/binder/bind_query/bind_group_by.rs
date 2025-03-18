use std::collections::BTreeSet;

use glaredb_error::{DbError, Result, not_implemented};
use glaredb_parser::ast;

use super::select_list::SelectList;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::column_binder::{DefaultColumnBinder, ExpressionColumnBinder};
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::binder::table_list::TableRef;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundGroupBy {
    pub expressions: Vec<Expression>,
    pub group_exprs_table: TableRef,
    pub grouping_sets: Vec<BTreeSet<usize>>,
}

#[derive(Debug)]
pub struct GroupByBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> GroupByBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        GroupByBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        group_by: ast::GroupByNode<ResolvedMeta>,
    ) -> Result<BoundGroupBy> {
        let sets = GroupByWithSets::try_from_ast(group_by)?;
        let group_table = bind_context.new_ephemeral_table()?;

        let mut column_binder = GroupByColumnBinder { select_list };
        let expr_binder = BaseExpressionBinder::new(self.current, self.resolve_context);

        let expressions = sets
            .expressions
            .into_iter()
            .enumerate()
            .map(|(idx, expr)| {
                let expr = expr_binder.bind_expression(
                    bind_context,
                    &expr,
                    &mut column_binder,
                    RecursionContext {
                        allow_windows: false,
                        allow_aggregates: false,
                        is_root: true,
                    },
                )?;

                let datatype = expr.datatype()?;
                bind_context.push_column_for_table(
                    group_table,
                    format!("__generated_group_{idx}"),
                    datatype,
                )?;

                Ok(expr)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(BoundGroupBy {
            expressions,
            group_exprs_table: group_table,
            grouping_sets: sets.grouping_sets,
        })
    }
}

#[derive(Debug)]
pub struct GroupByColumnBinder<'a> {
    select_list: &'a mut SelectList,
}

impl ExpressionColumnBinder for GroupByColumnBinder<'_> {
    fn bind_from_root_literal(
        &mut self,
        _bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>> {
        if let Some(col) = self.select_list.column_by_ordinal(bind_context, literal)? {
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

#[derive(Debug, Clone, PartialEq)]
struct GroupByWithSets {
    expressions: Vec<ast::Expr<ResolvedMeta>>,
    grouping_sets: Vec<BTreeSet<usize>>,
}

impl GroupByWithSets {
    fn try_from_ast(group_by: ast::GroupByNode<ResolvedMeta>) -> Result<Self> {
        match group_by {
            ast::GroupByNode::All => not_implemented!("GROUP BY ALL"),
            ast::GroupByNode::Exprs { mut exprs } => {
                let expr = match exprs.len() {
                    1 => exprs.pop().unwrap(),
                    _ => return Err(DbError::new("Invalid number of group by expressions")),
                };

                let (expressions, grouping_sets) = match expr {
                    ast::GroupByExpr::Expr(exprs) => {
                        let len = exprs.len();
                        (exprs, vec![(0..len).collect()])
                    }
                    ast::GroupByExpr::Rollup(exprs) => {
                        let len = exprs.len();
                        let mut sets: Vec<_> = (0..len).map(|i| (0..(len - i)).collect()).collect();
                        sets.push(BTreeSet::new()); // Empty set.
                        (exprs, sets)
                    }
                    ast::GroupByExpr::Cube(exprs) => {
                        let len = exprs.len();
                        let mut sets = Vec::new();

                        // Powerset
                        for mask in 0..(1 << len) {
                            let mut set = BTreeSet::new();
                            let mut bitset = mask;
                            while bitset > 0 {
                                let right: u64 = bitset & { !(bitset - 1) };
                                let idx = right.trailing_zeros() as usize;
                                set.insert(idx);
                                bitset &= bitset - 1;
                            }
                            sets.push(set);
                        }

                        (exprs, sets)
                    }
                    ast::GroupByExpr::GroupingSets(_exprs) => {
                        not_implemented!("GROUPING SETS")
                    }
                };

                Ok(GroupByWithSets {
                    expressions,
                    grouping_sets,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_by_with_sets_from_group_by_single() {
        // GROUP BY a
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Expr(vec![ast::Expr::Ident(
                ast::Ident::new_unquoted("a"),
            )])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![ast::Expr::Ident(ast::Ident::new_unquoted("a"))],
            grouping_sets: vec![[0].into()],
        };

        assert_eq!(expected, sets)
    }

    #[test]
    fn group_by_with_sets_from_group_by_many() {
        // GROUP BY a, b
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Expr(vec![
                ast::Expr::Ident(ast::Ident::new_unquoted("a")),
                ast::Expr::Ident(ast::Ident::new_unquoted("b")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::new_unquoted("a")),
                ast::Expr::Ident(ast::Ident::new_unquoted("b")),
            ],
            grouping_sets: vec![[0, 1].into()],
        };

        assert_eq!(expected, sets)
    }

    #[test]
    fn group_by_with_sets_from_rollup() {
        // GROUP BY ROLLUP a, b, c
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Rollup(vec![
                ast::Expr::Ident(ast::Ident::new_unquoted("a")),
                ast::Expr::Ident(ast::Ident::new_unquoted("b")),
                ast::Expr::Ident(ast::Ident::new_unquoted("c")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::new_unquoted("a")),
                ast::Expr::Ident(ast::Ident::new_unquoted("b")),
                ast::Expr::Ident(ast::Ident::new_unquoted("c")),
            ],
            grouping_sets: vec![[0, 1, 2].into(), [0, 1].into(), [0].into(), [].into()],
        };

        assert_eq!(expected, sets)
    }

    #[test]
    fn group_by_with_sets_from_cube() {
        // GROUP BY CUBE a, b, c
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Cube(vec![
                ast::Expr::Ident(ast::Ident::new_unquoted("a")),
                ast::Expr::Ident(ast::Ident::new_unquoted("b")),
                ast::Expr::Ident(ast::Ident::new_unquoted("c")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::new_unquoted("a")),
                ast::Expr::Ident(ast::Ident::new_unquoted("b")),
                ast::Expr::Ident(ast::Ident::new_unquoted("c")),
            ],
            grouping_sets: vec![
                [].into(),
                [0].into(),
                [1].into(),
                [0, 1].into(),
                [2].into(),
                [0, 2].into(),
                [1, 2].into(),
                [0, 1, 2].into(),
            ],
        };

        assert_eq!(expected, sets)
    }
}
