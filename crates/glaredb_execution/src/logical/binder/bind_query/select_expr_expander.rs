use std::collections::{HashMap, HashSet};

use glaredb_error::{RayexecError, Result, ResultExt};
use rayexec_parser::ast;
use regex::Regex;

use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::table_list::TableAlias;
use crate::logical::resolver::ResolvedMeta;

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
            ast::SelectExpr::Wildcard(modifier) => {
                let mut exprs = Vec::new();

                // Handle USING columns. Expanding a SELECT * query that
                // contains USING in the join, we only want to display the
                // column once.
                //
                // USING columns are listed first, followed by the other table
                // columns.
                let mut handled = HashSet::new();
                for using in self.bind_context.get_using_columns(self.current)? {
                    let reference = ColumnReference {
                        table_scope: using.table_ref,
                        column: using.col_idx,
                    };
                    let datatype = self.bind_context.get_column_type(reference)?;

                    exprs.push(ExpandedSelectExpr::Column {
                        expr: ColumnExpr {
                            reference,
                            datatype,
                        },
                        name: using.column.clone(),
                    });

                    handled.insert(&using.column);
                }

                for table in self.bind_context.iter_tables_in_scope(self.current)? {
                    for (col_idx, name) in table.column_names.iter().enumerate() {
                        // If column is already added from USING, skip it.
                        if handled.contains(name) {
                            continue;
                        }

                        let reference = ColumnReference {
                            table_scope: table.reference,
                            column: col_idx,
                        };
                        let datatype = self.bind_context.get_column_type(reference)?;

                        exprs.push(ExpandedSelectExpr::Column {
                            expr: ColumnExpr {
                                reference,
                                datatype,
                            },
                            name: name.clone(),
                        })
                    }
                }

                Self::exclude_and_replace_cols(&mut exprs, modifier)?;

                exprs
            }
            ast::SelectExpr::QualifiedWildcard(reference, modifier) => {
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
                    .iter_tables_in_scope(self.current)?
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
                debug_assert_eq!(table.column_names.len(), table.column_types.len());

                for (col_idx, (name, datatype)) in table.iter_names_and_types().enumerate() {
                    exprs.push(ExpandedSelectExpr::Column {
                        expr: ColumnExpr {
                            reference: ColumnReference {
                                table_scope: table.reference,
                                column: col_idx,
                            },
                            datatype: datatype.clone(),
                        },
                        name: name.to_string(),
                    })
                }

                Self::exclude_and_replace_cols(&mut exprs, modifier)?;

                exprs
            }
            ast::SelectExpr::AliasedExpr(expr, alias) => {
                vec![ExpandedSelectExpr::Expr {
                    expr,
                    alias: Some(alias.into_normalized_string()),
                }]
            }
            ast::SelectExpr::Expr(expr) => {
                // Check if this is a COLUMNS expr, expand if it is.
                if let ast::Expr::Columns(cols_expr) = expr {
                    match cols_expr {
                        ast::ColumnsExpr::Pattern(pattern) => {
                            let regex = Regex::new(&pattern)
                                .context("Failed to build column regex from pattern")?;

                            let mut exprs = Vec::new();
                            // Iter all columns in the context, select the ones
                            // that match the regex.
                            for table in self.bind_context.iter_tables_in_scope(self.current)? {
                                for (col_idx, (name, datatype)) in
                                    table.iter_names_and_types().enumerate()
                                {
                                    if !regex.is_match(name) {
                                        continue;
                                    }

                                    exprs.push(ExpandedSelectExpr::Column {
                                        expr: ColumnExpr {
                                            reference: ColumnReference {
                                                table_scope: table.reference,
                                                column: col_idx,
                                            },
                                            datatype: datatype.clone(),
                                        },
                                        name: name.to_string(),
                                    })
                                }
                            }

                            exprs
                        }
                    }
                } else {
                    // Normal expression.
                    vec![ExpandedSelectExpr::Expr { expr, alias: None }]
                }
            }
        })
    }

    fn exclude_and_replace_cols(
        exprs: &mut Vec<ExpandedSelectExpr>,
        modifier: ast::WildcardModifier<ResolvedMeta>,
    ) -> Result<()> {
        // Handles exclusion first, then replace. Attempting to replace a column
        // that's been excluded should error.

        // Normalizes excluded columns, includes a boolean to track if we
        // visited this column.
        //
        // We do not allow users to exlude columns that don't exist in the output, so
        // error if any of the exluded columns aren't visited.
        let mut normalized_excluded: HashMap<String, bool> = modifier
            .exclude_cols
            .into_iter()
            .map(|ident| (ident.into_normalized_string(), false))
            .collect();

        exprs.retain(|expr| {
            if let ExpandedSelectExpr::Column { name, .. } = expr {
                if let Some(visited) = normalized_excluded.get_mut(name) {
                    // Column excluded.
                    *visited = true;
                    return false; // Don't retain.
                }
            }
            true
        });

        for (name, visited) in normalized_excluded {
            if !visited {
                return Err(RayexecError::new(format!(
                    "Column \"{name}\" was in EXCLUDE list, but it's not a column being returned"
                )));
            }
        }

        // Like above, we track if we've visited a replacement column, and error
        // if we don't.
        let mut normalized_replaces: HashMap<String, (ast::Expr<ResolvedMeta>, bool)> = modifier
            .replace_cols
            .into_iter()
            .map(|replacement| {
                (
                    replacement.col.into_normalized_string(),
                    (replacement.expr, false),
                )
            })
            .collect();

        for expr in exprs {
            if let ExpandedSelectExpr::Column { name, .. } = expr {
                if let Some((ast_expr, visited)) = normalized_replaces.get_mut(name) {
                    // Column should be replaced, just clone the replacement ast
                    // expr.
                    //
                    // While we may end up replacing multiple cols with the same
                    // ast expr, we don't want to check for that here. It'll
                    // eventually go through the same ambiguity/duplicate name
                    // checks during planning.
                    *expr = ExpandedSelectExpr::Expr {
                        expr: ast_expr.clone(),
                        alias: Some(name.clone()),
                    };

                    // Mark visited.
                    *visited = true;
                }
            }
        }

        for (name, (_, visited)) in normalized_replaces {
            if !visited {
                return Err(RayexecError::new(format!(
                    "Column \"{name}\" was in REPLACE list, but it's not a column being returned"
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ast::ObjectReference;

    use super::*;
    use crate::arrays::datatype::DataType;

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

        let exprs = vec![ast::SelectExpr::Wildcard(ast::WildcardModifier {
            exclude_cols: Vec::new(),
            replace_cols: Vec::new(),
        })];

        let expected = vec![
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    reference: ColumnReference {
                        table_scope: table_ref,
                        column: 0,
                    },
                    datatype: DataType::Utf8,
                },
                name: "c1".to_string(),
            },
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    reference: ColumnReference {
                        table_scope: table_ref,
                        column: 1,
                    },
                    datatype: DataType::Utf8,
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
            ObjectReference(vec![ast::Ident::new_unquoted("t1")]),
            ast::WildcardModifier {
                exclude_cols: Vec::new(),
                replace_cols: Vec::new(),
            },
        )];

        let expected = vec![
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    reference: ColumnReference {
                        table_scope: t1_table_ref,
                        column: 0,
                    },
                    datatype: DataType::Utf8,
                },
                name: "c1".to_string(),
            },
            ExpandedSelectExpr::Column {
                expr: ColumnExpr {
                    reference: ColumnReference {
                        table_scope: t1_table_ref,
                        column: 1,
                    },
                    datatype: DataType::Utf8,
                },
                name: "c2".to_string(),
            },
        ];
        let expanded = expander.expand_all_select_exprs(exprs).unwrap();

        assert_eq!(expected, expanded);
    }
}
