use crate::{
    expr::Expression,
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef, TableRef},
            column_binder::{DefaultColumnBinder, ExpressionColumnBinder},
            expr_binder::{BaseExpressionBinder, RecursionContext},
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast;
use std::collections::BTreeSet;

use super::select_list::SelectList;

#[derive(Debug, Clone, PartialEq)]
pub struct BoundGroupBy {
    pub expressions: Vec<Expression>,
    pub group_table: TableRef,
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

                let datatype = expr.datatype(bind_context)?;
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
            group_table,
            grouping_sets: sets.grouping_sets,
        })
    }
}

#[derive(Debug)]
pub struct GroupByColumnBinder<'a> {
    select_list: &'a mut SelectList,
}

impl<'a> ExpressionColumnBinder for GroupByColumnBinder<'a> {
    fn bind_from_root_literal(
        &mut self,
        _bind_scope: BindScopeRef,
        _bind_context: &mut BindContext,
        literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>> {
        if let Some(col) = self.select_list.column_by_ordinal(literal)? {
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
        if let Some(col) = self.select_list.column_by_user_alias(ident) {
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
                    _ => return Err(RayexecError::new("Invalid number of group by expressions")),
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
                ast::Ident::from_string("a"),
            )])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![ast::Expr::Ident(ast::Ident::from_string("a"))],
            grouping_sets: vec![[0].into()],
        };

        assert_eq!(expected, sets)
    }

    #[test]
    fn group_by_with_sets_from_group_by_many() {
        // GROUP BY a, b
        let node = ast::GroupByNode::Exprs {
            exprs: vec![ast::GroupByExpr::Expr(vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
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
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
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
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
            ])],
        };

        let sets = GroupByWithSets::try_from_ast(node).unwrap();
        let expected = GroupByWithSets {
            expressions: vec![
                ast::Expr::Ident(ast::Ident::from_string("a")),
                ast::Expr::Ident(ast::Ident::from_string("b")),
                ast::Expr::Ident(ast::Ident::from_string("c")),
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
