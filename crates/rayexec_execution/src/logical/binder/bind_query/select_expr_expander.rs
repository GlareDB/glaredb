use std::collections::HashSet;

use crate::{
    expr::column_expr::ColumnExpr,
    logical::{
        binder::bind_context::{BindContext, BindScopeRef, TableAlias},
        resolver::ResolvedMeta,
    },
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

/// An expanded select expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ExpandedSelectExpr {
    /// A typical expression. Can be a reference to a column, or a more complex
    /// expression.
    Expr {
        /// The original AST expression that should go through regular
        /// expression binding.
        expr: ast::Expr<ResolvedMeta>,
        /// Optional user-provided alias.
        alias: Option<String>,
    },
    /// An index of a column in the current scope. This is needed for wildcards
    /// since they're expanded to match some number of columns in the current
    /// scope.
    Column {
        /// The column expression representing a column in some scope.
        expr: ColumnExpr,
        /// Name as it existed in the bind scope.
        name: String,
    },
}

impl ExpandedSelectExpr {
    pub fn get_alias(&self) -> Option<&str> {
        match self {
            Self::Expr { alias, .. } => alias.as_ref().map(|a| a.as_str()),
            Self::Column { .. } => None,
        }
    }
}

/// Expands wildcards in expressions found in the select list.
///
/// Generates ast expressions.
#[derive(Debug)]
pub struct SelectExprExpander<'a> {
    pub current: BindScopeRef,
    pub bind_context: &'a BindContext,
}

impl<'a> SelectExprExpander<'a> {
    pub fn new(current: BindScopeRef, bind_context: &'a BindContext) -> Self {
        SelectExprExpander {
            current,
            bind_context,
        }
    }

    pub fn expand_all_select_exprs(
        &self,
        exprs: impl IntoIterator<Item = ast::SelectExpr<ResolvedMeta>>,
    ) -> Result<Vec<ExpandedSelectExpr>> {
        let mut expanded = Vec::new();
        for expr in exprs {
            let mut ex = self.expand_select_expr(expr)?;
            expanded.append(&mut ex);
        }
        Ok(expanded)
    }

    pub fn expand_select_expr(
        &self,
        expr: ast::SelectExpr<ResolvedMeta>,
    ) -> Result<Vec<ExpandedSelectExpr>> {
        Ok(match expr {
            ast::SelectExpr::Wildcard(_wildcard) => {
                // TODO: Exclude, replace
                let mut exprs = Vec::new();

                // Handle USING columns. Expanding a SELECT * query that
                // contains USING in the join, we only want to display the
                // column once.
                //
                // USING columns are listed first, followed by the other table
                // columns.
                let mut handled = HashSet::new();
                for using in self.bind_context.get_using_columns(self.current)? {
                    exprs.push(ExpandedSelectExpr::Column {
                        expr: ColumnExpr {
                            table_scope: using.table_ref,
                            column: using.col_idx,
                        },
                        name: using.column.clone(),
                    });

                    handled.insert(&using.column);
                }

                for table in self.bind_context.iter_tables(self.current)? {
                    for (col_idx, name) in table.column_names.iter().enumerate() {
                        // If column is already added from USING, skip it.
                        if handled.contains(name) {
                            continue;
                        }

                        exprs.push(ExpandedSelectExpr::Column {
                            expr: ColumnExpr {
                                table_scope: table.reference,
                                column: col_idx,
                            },
                            name: name.clone(),
                        })
                    }
                }

                exprs
            }
            ast::SelectExpr::QualifiedWildcard(reference, _wildcard) => {
                // TODO: Exclude, replace
                if reference.0.len() > 1 {
                    return Err(RayexecError::new(
                        "Qualified wildcard references with more than one ident not yet supported",
                    ));
                }

                // TODO: Get schema + catalog too if they exist.
                let table = reference.base()?.into_normalized_string();
                let alias = TableAlias {
                    database: None,
                    schema: None,
                    table,
                };

                let table = self
                    .bind_context
                    .iter_tables(self.current)?
                    .find(|t| match &t.alias {
                        Some(have_alias) => have_alias.matches(&alias),
                        None => false,
                    })
                    .ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing table '{alias}', cannot expand wildcard"
                        ))
                    })?;

                let mut exprs = Vec::new();
                for (col_idx, name) in table.column_names.iter().enumerate() {
                    exprs.push(ExpandedSelectExpr::Column {
                        expr: ColumnExpr {
                            table_scope: table.reference,
                            column: col_idx,
                        },
                        name: name.clone(),
                    })
                }

                exprs
            }
            ast::SelectExpr::AliasedExpr(expr, alias) => {
                vec![ExpandedSelectExpr::Expr {
                    expr,
                    alias: Some(alias.into_normalized_string()),
                }]
            }
            ast::SelectExpr::Expr(expr) => {
                vec![ExpandedSelectExpr::Expr { expr, alias: None }]
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use ast::ObjectReference;
    use rayexec_bullet::datatype::DataType;

    use super::*;

    #[test]
    fn expand_none() {
        let bind_context = BindContext::new();
        let expander = SelectExprExpander::new(bind_context.root_scope_ref(), &bind_context);

        let exprs = vec![
            ast::SelectExpr::Expr(ast::Expr::Literal(ast::Literal::Number("1".to_string()))),
            ast::SelectExpr::Expr(ast::Expr::Literal(ast::Literal::Number("2".to_string()))),
        ];

        let expected = vec![
            ExpandedSelectExpr::Expr {
                expr: ast::Expr::Literal(ast::Literal::Number("1".to_string())),
                alias: None,
            },
            ExpandedSelectExpr::Expr {
                expr: ast::Expr::Literal(ast::Literal::Number("2".to_string())),
                alias: None,
            },
        ];
        let expanded = expander.expand_all_select_exprs(exprs).unwrap();

        assert_eq!(expected, expanded);
    }

    #[test]
    fn expand_unqualified() {
        let mut bind_context = BindContext::new();
        let table_ref = bind_context
            .push_table(
                bind_context.root_scope_ref(),
                Some(TableAlias {
                    database: Some("d1".to_string()),
                    schema: Some("s1".to_string()),
                    table: "t1".to_string(),
                }),
                vec![DataType::Utf8, DataType::Utf8],
                vec!["c1".to_string(), "c2".to_string()],
            )
            .unwrap();

        let expander = SelectExprExpander::new(bind_context.root_scope_ref(), &bind_context);

        let exprs = vec![ast::SelectExpr::Wildcard(ast::Wildcard::default())];

        let expected = vec![
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    table_scope: table_ref,
                    column: 0,
                },
                name: "c1".to_string(),
            },
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    table_scope: table_ref,
                    column: 1,
                },
                name: "c2".to_string(),
            },
        ];
        let expanded = expander.expand_all_select_exprs(exprs).unwrap();

        assert_eq!(expected, expanded);
    }

    #[test]
    fn expand_qualified() {
        let mut bind_context = BindContext::new();
        // Add 't1'
        let t1_table_ref = bind_context
            .push_table(
                bind_context.root_scope_ref(),
                Some(TableAlias {
                    database: Some("d1".to_string()),
                    schema: Some("s1".to_string()),
                    table: "t1".to_string(),
                }),
                vec![DataType::Utf8, DataType::Utf8],
                vec!["c1".to_string(), "c2".to_string()],
            )
            .unwrap();
        // Add 't2'
        bind_context
            .push_table(
                bind_context.root_scope_ref(),
                Some(TableAlias {
                    database: Some("d1".to_string()),
                    schema: Some("s1".to_string()),
                    table: "t2".to_string(),
                }),
                vec![DataType::Utf8, DataType::Utf8],
                vec!["c3".to_string(), "c4".to_string()],
            )
            .unwrap();

        let expander = SelectExprExpander::new(bind_context.root_scope_ref(), &bind_context);

        // Expand just 't1'
        let exprs = vec![ast::SelectExpr::QualifiedWildcard(
            ObjectReference(vec![ast::Ident::from_string("t1")]),
            ast::Wildcard::default(),
        )];

        let expected = vec![
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    table_scope: t1_table_ref,
                    column: 0,
                },
                name: "c1".to_string(),
            },
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    table_scope: t1_table_ref,
                    column: 1,
                },
                name: "c2".to_string(),
            },
        ];
        let expanded = expander.expand_all_select_exprs(exprs).unwrap();

        assert_eq!(expected, expanded);
    }
}
