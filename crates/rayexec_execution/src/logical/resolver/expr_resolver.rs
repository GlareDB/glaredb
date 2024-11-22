use std::collections::HashMap;

use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast::{self, FunctionArg, ReplaceColumn};
use rayexec_parser::meta::Raw;

use super::resolve_normal::create_user_facing_resolve_err;
use super::resolved_function::ResolvedFunction;
use super::{ResolveContext, ResolvedMeta, Resolver};
use crate::database::catalog_entry::CatalogEntryType;
use crate::functions::table::TableFunctionArgs;
use crate::logical::binder::expr_binder::BaseExpressionBinder;
use crate::logical::operator::LocationRequirement;

pub struct ExpressionResolver<'a> {
    resolver: &'a Resolver<'a>,
}

impl<'a> ExpressionResolver<'a> {
    pub fn new(resolver: &'a Resolver) -> Self {
        ExpressionResolver { resolver }
    }

    pub async fn resolve_select_expr(
        &self,
        select_expr: ast::SelectExpr<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::SelectExpr<ResolvedMeta>> {
        match select_expr {
            ast::SelectExpr::Expr(expr) => Ok(ast::SelectExpr::Expr(
                self.resolve_expression(expr, resolve_context).await?,
            )),
            ast::SelectExpr::AliasedExpr(expr, alias) => Ok(ast::SelectExpr::AliasedExpr(
                self.resolve_expression(expr, resolve_context).await?,
                alias,
            )),
            ast::SelectExpr::QualifiedWildcard(object_name, wildcard) => {
                Ok(ast::SelectExpr::QualifiedWildcard(
                    object_name,
                    self.resolve_wildcard(wildcard, resolve_context).await?,
                ))
            }
            ast::SelectExpr::Wildcard(wildcard) => Ok(ast::SelectExpr::Wildcard(
                self.resolve_wildcard(wildcard, resolve_context).await?,
            )),
        }
    }

    pub async fn resolve_wildcard(
        &self,
        wildcard: ast::WildcardModifier<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::WildcardModifier<ResolvedMeta>> {
        let mut replace_cols = Vec::with_capacity(wildcard.replace_cols.len());
        for replace in wildcard.replace_cols {
            replace_cols.push(ReplaceColumn {
                col: replace.col,
                expr: self
                    .resolve_expression(replace.expr, resolve_context)
                    .await?,
            });
        }

        Ok(ast::WildcardModifier {
            exclude_cols: wildcard.exclude_cols,
            replace_cols,
        })
    }

    pub async fn resolve_group_by_expr(
        &self,
        expr: ast::GroupByExpr<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::GroupByExpr<ResolvedMeta>> {
        Ok(match expr {
            ast::GroupByExpr::Expr(exprs) => {
                ast::GroupByExpr::Expr(self.resolve_expressions(exprs, resolve_context).await?)
            }
            ast::GroupByExpr::Cube(exprs) => {
                ast::GroupByExpr::Cube(self.resolve_expressions(exprs, resolve_context).await?)
            }
            ast::GroupByExpr::Rollup(exprs) => {
                ast::GroupByExpr::Rollup(self.resolve_expressions(exprs, resolve_context).await?)
            }
            ast::GroupByExpr::GroupingSets(exprs) => ast::GroupByExpr::GroupingSets(
                self.resolve_expressions(exprs, resolve_context).await?,
            ),
        })
    }

    /// Resolves functions arguments for a table function.
    ///
    /// Slightly different from normal argument resolving since arguments to a
    /// table function are more restrictive. E.g. we only allow literals as
    /// arguments.
    ///
    /// Note in the future we could allow more complex expressions as arguments,
    /// and we could support table function that accept columns as inputs.
    pub async fn resolve_table_function_args(
        &self,
        args: Vec<FunctionArg<Raw>>,
    ) -> Result<TableFunctionArgs> {
        let resolve_context = &mut ResolveContext::default(); // Empty resolve context since we don't allow complex expressions.

        let mut named = HashMap::new();
        let mut positional = Vec::new();

        for func_arg in args {
            match func_arg {
                ast::FunctionArg::Named { name, arg } => {
                    let name = name.into_normalized_string();
                    let arg = match arg {
                        ast::FunctionArgExpr::Wildcard => {
                            return Err(RayexecError::new(
                                "Cannot use '*' as an argument to a table function",
                            ))
                        }
                        ast::FunctionArgExpr::Expr(expr) => {
                            match Box::pin(self.resolve_expression(expr, resolve_context)).await? {
                                ast::Expr::Literal(lit) => {
                                    BaseExpressionBinder::bind_literal(&lit)?.try_into_scalar()?
                                }
                                other => {
                                    return Err(RayexecError::new(format!(
                                        "Table function arguments must be constant, got {other:?}"
                                    )))
                                }
                            }
                        }
                    };

                    if named.contains_key(&name) {
                        return Err(RayexecError::new(format!("Duplicate argument: {name}")));
                    }
                    named.insert(name, arg);
                }
                FunctionArg::Unnamed { arg } => {
                    let arg = match arg {
                        ast::FunctionArgExpr::Wildcard => {
                            return Err(RayexecError::new(
                                "Cannot use '*' as an argument to a table function",
                            ))
                        }
                        ast::FunctionArgExpr::Expr(expr) => {
                            match Box::pin(self.resolve_expression(expr, resolve_context)).await? {
                                ast::Expr::Literal(lit) => {
                                    BaseExpressionBinder::bind_literal(&lit)?.try_into_scalar()?
                                }
                                other => {
                                    return Err(RayexecError::new(format!(
                                        "Table function arguments must be constant, got {other:?}"
                                    )))
                                }
                            }
                        }
                    };
                    positional.push(arg);
                }
            }
        }

        Ok(TableFunctionArgs { named, positional })
    }

    pub async fn resolve_expressions(
        &self,
        exprs: impl IntoIterator<Item = ast::Expr<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<Vec<ast::Expr<ResolvedMeta>>> {
        let mut resolved = Vec::new();
        for expr in exprs {
            resolved.push(self.resolve_expression(expr, resolve_context).await?);
        }
        Ok(resolved)
    }

    /// Resolve an expression.
    pub async fn resolve_expression(
        &self,
        expr: ast::Expr<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
        match expr {
            ast::Expr::Ident(ident) => Ok(ast::Expr::Ident(ident)),
            ast::Expr::CompoundIdent(idents) => Ok(ast::Expr::CompoundIdent(idents)),
            ast::Expr::Literal(lit) => Ok(ast::Expr::Literal(match lit {
                ast::Literal::Number(s) => ast::Literal::Number(s),
                ast::Literal::SingleQuotedString(s) => ast::Literal::SingleQuotedString(s),
                ast::Literal::Boolean(b) => ast::Literal::Boolean(b),
                ast::Literal::Null => ast::Literal::Null,
                ast::Literal::Struct { keys, values } => {
                    let resolved =
                        Box::pin(self.resolve_expressions(values, resolve_context)).await?;
                    ast::Literal::Struct {
                        keys,
                        values: resolved,
                    }
                }
            })),
            ast::Expr::Array(arr) => {
                let mut new_arr = Vec::with_capacity(arr.len());
                for v in arr {
                    let new_v = Box::pin(self.resolve_expression(v, resolve_context)).await?;
                    new_arr.push(new_v);
                }
                Ok(ast::Expr::Array(new_arr))
            }
            ast::Expr::ArraySubscript { expr, subscript } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                let subscript = match *subscript {
                    ast::ArraySubscript::Index(index) => ast::ArraySubscript::Index(
                        Box::pin(self.resolve_expression(index, resolve_context)).await?,
                    ),
                    ast::ArraySubscript::Slice {
                        lower,
                        upper,
                        stride,
                    } => {
                        let lower = match lower {
                            Some(lower) => Some(
                                Box::pin(self.resolve_expression(lower, resolve_context)).await?,
                            ),
                            None => None,
                        };
                        let upper = match upper {
                            Some(upper) => Some(
                                Box::pin(self.resolve_expression(upper, resolve_context)).await?,
                            ),
                            None => None,
                        };
                        let stride = match stride {
                            Some(stride) => Some(
                                Box::pin(self.resolve_expression(stride, resolve_context)).await?,
                            ),
                            None => None,
                        };

                        ast::ArraySubscript::Slice {
                            lower,
                            upper,
                            stride,
                        }
                    }
                };

                Ok(ast::Expr::ArraySubscript {
                    expr: Box::new(expr),
                    subscript: Box::new(subscript),
                })
            }
            ast::Expr::UnaryExpr { op, expr } => {
                match op {
                    ast::UnaryOperator::Plus => {
                        // Nothing to do, just bind and return the inner expression.
                        Box::pin(self.resolve_expression(*expr, resolve_context)).await
                    }
                    ast::UnaryOperator::Minus => match *expr {
                        ast::Expr::Literal(ast::Literal::Number(n)) => {
                            Ok(ast::Expr::Literal(ast::Literal::Number(format!("-{n}"))))
                        }
                        expr => Ok(ast::Expr::UnaryExpr {
                            op: ast::UnaryOperator::Minus,
                            expr: Box::new(
                                Box::pin(self.resolve_expression(expr, resolve_context)).await?,
                            ),
                        }),
                    },
                    ast::UnaryOperator::Not => {
                        let expr =
                            Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                        Ok(ast::Expr::UnaryExpr {
                            op,
                            expr: Box::new(expr),
                        })
                    }
                }
            }
            ast::Expr::BinaryExpr { left, op, right } => Ok(ast::Expr::BinaryExpr {
                left: Box::new(Box::pin(self.resolve_expression(*left, resolve_context)).await?),
                op,
                right: Box::new(Box::pin(self.resolve_expression(*right, resolve_context)).await?),
            }),
            ast::Expr::Function(func) => {
                // TODO: Search path (with system being the first to check)
                if func.reference.0.len() != 1 {
                    return Err(RayexecError::new(
                        "Qualified function names not yet supported",
                    ));
                }
                let func_name = &func.reference.0[0].as_normalized_string();
                let catalog = "system";
                let schema = "glare_catalog";

                let filter = match func.filter {
                    Some(filter) => Some(Box::new(
                        Box::pin(self.resolve_expression(*filter, resolve_context)).await?,
                    )),
                    None => None,
                };

                let mut args = Vec::with_capacity(func.args.len());
                // TODO: This current rewrites '*' function arguments to 'true'.
                // This is for 'count(*)'. What we should be doing is rewriting
                // 'count(*)' to 'count_star()' and have a function
                // implementation for 'count_star'.
                //
                // No other function accepts a '*' (I think).
                for func_arg in func.args {
                    let func_arg = match func_arg {
                        ast::FunctionArg::Named { name, arg } => ast::FunctionArg::Named {
                            name,
                            arg: match arg {
                                ast::FunctionArgExpr::Wildcard => ast::FunctionArgExpr::Expr(
                                    ast::Expr::Literal(ast::Literal::Boolean(true)),
                                ),
                                ast::FunctionArgExpr::Expr(expr) => ast::FunctionArgExpr::Expr(
                                    Box::pin(self.resolve_expression(expr, resolve_context))
                                        .await?,
                                ),
                            },
                        },
                        ast::FunctionArg::Unnamed { arg } => ast::FunctionArg::Unnamed {
                            arg: match arg {
                                ast::FunctionArgExpr::Wildcard => ast::FunctionArgExpr::Expr(
                                    ast::Expr::Literal(ast::Literal::Boolean(true)),
                                ),
                                ast::FunctionArgExpr::Expr(expr) => ast::FunctionArgExpr::Expr(
                                    Box::pin(self.resolve_expression(expr, resolve_context))
                                        .await?,
                                ),
                            },
                        },
                    };
                    args.push(func_arg);
                }

                let schema_ent = self
                    .resolver
                    .context
                    .get_database(catalog)?
                    .catalog
                    .get_schema(self.resolver.tx, schema)?
                    .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;

                // Check scalars first.
                if let Some(scalar) = schema_ent.get_scalar_function(self.resolver.tx, func_name)? {
                    // TODO: Allow unresolved scalars?
                    // TODO: This also assumes scalars (and aggs) are the same everywhere, which
                    // they probably should be for now.
                    let resolve_idx = resolve_context.functions.push_resolved(
                        ResolvedFunction::Scalar(
                            scalar.try_as_scalar_function_entry()?.function.clone(),
                        ),
                        LocationRequirement::Any,
                    );
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: resolve_idx,
                        distinct: func.distinct,
                        args,
                        filter,
                    }));
                }

                // Now check aggregates.
                if let Some(aggregate) =
                    schema_ent.get_aggregate_function(self.resolver.tx, func_name)?
                {
                    // TODO: Allow unresolved aggregates?
                    let resolve_idx = resolve_context.functions.push_resolved(
                        ResolvedFunction::Aggregate(
                            aggregate
                                .try_as_aggregate_function_entry()?
                                .function
                                .clone(),
                        ),
                        LocationRequirement::Any,
                    );
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: resolve_idx,
                        distinct: func.distinct,
                        args,
                        filter,
                    }));
                }

                Err(create_user_facing_resolve_err(
                    self.resolver.tx,
                    Some(&schema_ent),
                    &[
                        CatalogEntryType::ScalarFunction,
                        CatalogEntryType::AggregateFunction,
                    ],
                    func_name,
                ))
            }
            ast::Expr::Subquery(subquery) => {
                let resolved =
                    Box::pin(self.resolver.resolve_query(*subquery, resolve_context)).await?;
                Ok(ast::Expr::Subquery(Box::new(resolved)))
            }
            ast::Expr::Exists {
                subquery,
                not_exists,
            } => {
                let resolved =
                    Box::pin(self.resolver.resolve_query(*subquery, resolve_context)).await?;
                Ok(ast::Expr::Exists {
                    subquery: Box::new(resolved),
                    not_exists,
                })
            }
            ast::Expr::TypedString { datatype, value } => {
                let datatype = Resolver::ast_datatype_to_exec_datatype(datatype)?;
                Ok(ast::Expr::TypedString { datatype, value })
            }
            ast::Expr::Cast { datatype, expr } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                let datatype = Resolver::ast_datatype_to_exec_datatype(datatype)?;
                Ok(ast::Expr::Cast {
                    datatype,
                    expr: Box::new(expr),
                })
            }
            ast::Expr::Nested(expr) => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                Ok(ast::Expr::Nested(Box::new(expr)))
            }
            ast::Expr::Interval(ast::Interval {
                value,
                leading,
                trailing,
            }) => {
                let value = Box::pin(self.resolve_expression(*value, resolve_context)).await?;
                Ok(ast::Expr::Interval(ast::Interval {
                    value: Box::new(value),
                    leading,
                    trailing,
                }))
            }
            ast::Expr::Like {
                negated: not_like,
                case_insensitive,
                expr,
                pattern,
            } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                let pattern = Box::pin(self.resolve_expression(*pattern, resolve_context)).await?;
                Ok(ast::Expr::Like {
                    negated: not_like,
                    case_insensitive,
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                })
            }
            ast::Expr::IsNull { expr, negated } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                Ok(ast::Expr::IsNull {
                    expr: Box::new(expr),
                    negated,
                })
            }
            ast::Expr::IsBool { expr, val, negated } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                Ok(ast::Expr::IsBool {
                    expr: Box::new(expr),
                    val,
                    negated,
                })
            }
            ast::Expr::Between {
                negated,
                expr,
                low,
                high,
            } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                let low = Box::pin(self.resolve_expression(*low, resolve_context)).await?;
                let high = Box::pin(self.resolve_expression(*high, resolve_context)).await?;
                Ok(ast::Expr::Between {
                    negated,
                    expr: Box::new(expr),
                    low: Box::new(low),
                    high: Box::new(high),
                })
            }
            ast::Expr::AnySubquery { left, op, right } => {
                let left = Box::pin(self.resolve_expression(*left, resolve_context)).await?;
                let right = Box::pin(self.resolver.resolve_query(*right, resolve_context)).await?;
                Ok(ast::Expr::AnySubquery {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            ast::Expr::AllSubquery { left, op, right } => {
                let left = Box::pin(self.resolve_expression(*left, resolve_context)).await?;
                let right = Box::pin(self.resolver.resolve_query(*right, resolve_context)).await?;
                Ok(ast::Expr::AllSubquery {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            ast::Expr::InSubquery {
                negated,
                expr,
                subquery,
            } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                let subquery =
                    Box::pin(self.resolver.resolve_query(*subquery, resolve_context)).await?;
                Ok(ast::Expr::InSubquery {
                    negated,
                    expr: Box::new(expr),
                    subquery: Box::new(subquery),
                })
            }
            ast::Expr::InList {
                negated,
                expr,
                list,
            } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                let list = Box::pin(self.resolve_expressions(list, resolve_context)).await?;
                Ok(ast::Expr::InList {
                    negated,
                    expr: Box::new(expr),
                    list,
                })
            }
            ast::Expr::Case {
                expr,
                conditions,
                results,
                else_expr,
            } => {
                let expr = match expr {
                    Some(expr) => Some(Box::new(
                        Box::pin(self.resolve_expression(*expr, resolve_context)).await?,
                    )),
                    None => None,
                };
                let else_expr = match else_expr {
                    Some(expr) => Some(Box::new(
                        Box::pin(self.resolve_expression(*expr, resolve_context)).await?,
                    )),
                    None => None,
                };
                let conditions =
                    Box::pin(self.resolve_expressions(conditions, resolve_context)).await?;
                let results = Box::pin(self.resolve_expressions(results, resolve_context)).await?;
                Ok(ast::Expr::Case {
                    expr,
                    conditions,
                    results,
                    else_expr,
                })
            }
            ast::Expr::Substring { expr, from, count } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                let from = Box::pin(self.resolve_expression(*from, resolve_context)).await?;
                let count = match count {
                    Some(count) => {
                        Some(Box::pin(self.resolve_expression(*count, resolve_context)).await?)
                    }
                    None => None,
                };

                Ok(ast::Expr::Substring {
                    expr: Box::new(expr),
                    from: Box::new(from),
                    count: count.map(Box::new),
                })
            }
            ast::Expr::Extract { date_part, expr } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                Ok(ast::Expr::Extract {
                    date_part,
                    expr: Box::new(expr),
                })
            }
            other => not_implemented!("resolve expr {other:?}"),
        }
    }
}
