use std::collections::HashMap;

use crate::{
    expr::scalar::UnaryOperator,
    functions::table::TableFunctionArgs,
    logical::{operator::LocationRequirement, planner::plan_expr::ExpressionContext},
};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::{
    ast::{self, FunctionArg, ReplaceColumn},
    meta::Raw,
};

use super::{bound_function::BoundFunction, BindData, Binder, Bound};

pub struct ExpressionBinder<'a> {
    binder: &'a Binder<'a>,
}

impl<'a> ExpressionBinder<'a> {
    pub fn new(binder: &'a Binder) -> Self {
        ExpressionBinder { binder }
    }

    pub async fn bind_select_expr(
        &self,
        select_expr: ast::SelectExpr<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::SelectExpr<Bound>> {
        match select_expr {
            ast::SelectExpr::Expr(expr) => Ok(ast::SelectExpr::Expr(
                self.bind_expression(expr, bind_data).await?,
            )),
            ast::SelectExpr::AliasedExpr(expr, alias) => Ok(ast::SelectExpr::AliasedExpr(
                self.bind_expression(expr, bind_data).await?,
                alias,
            )),
            ast::SelectExpr::QualifiedWildcard(object_name, wildcard) => {
                Ok(ast::SelectExpr::QualifiedWildcard(
                    object_name,
                    self.bind_wildcard(wildcard, bind_data).await?,
                ))
            }
            ast::SelectExpr::Wildcard(wildcard) => Ok(ast::SelectExpr::Wildcard(
                self.bind_wildcard(wildcard, bind_data).await?,
            )),
        }
    }

    pub async fn bind_wildcard(
        &self,
        wildcard: ast::Wildcard<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::Wildcard<Bound>> {
        let mut replace_cols = Vec::with_capacity(wildcard.replace_cols.len());
        for replace in wildcard.replace_cols {
            replace_cols.push(ReplaceColumn {
                col: replace.col,
                expr: self.bind_expression(replace.expr, bind_data).await?,
            });
        }

        Ok(ast::Wildcard {
            exclude_cols: wildcard.exclude_cols,
            replace_cols,
        })
    }

    pub async fn bind_group_by_expr(
        &self,
        expr: ast::GroupByExpr<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::GroupByExpr<Bound>> {
        Ok(match expr {
            ast::GroupByExpr::Expr(exprs) => {
                ast::GroupByExpr::Expr(self.bind_expressions(exprs, bind_data).await?)
            }
            ast::GroupByExpr::Cube(exprs) => {
                ast::GroupByExpr::Cube(self.bind_expressions(exprs, bind_data).await?)
            }
            ast::GroupByExpr::Rollup(exprs) => {
                ast::GroupByExpr::Rollup(self.bind_expressions(exprs, bind_data).await?)
            }
            ast::GroupByExpr::GroupingSets(exprs) => {
                ast::GroupByExpr::GroupingSets(self.bind_expressions(exprs, bind_data).await?)
            }
        })
    }

    /// Binds functions arguments for a table function.
    ///
    /// Slightly different from normal argument binding since arguments to a
    /// table function are more restrictive. E.g. we only allow literals as
    /// arguments.
    ///
    /// Note in the future we could allow more complex expressions as arguments,
    /// and we could support table function that accept columns as inputs.
    pub async fn bind_table_function_args(
        &self,
        args: Vec<FunctionArg<Raw>>,
    ) -> Result<TableFunctionArgs> {
        let bind_data = &mut BindData::default(); // Empty bind data since we don't allow complex expressions.

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
                            match Box::pin(self.bind_expression(expr, bind_data)).await? {
                                ast::Expr::Literal(lit) => {
                                    ExpressionContext::plan_literal(lit)?.try_into_scalar()?
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
                            match Box::pin(self.bind_expression(expr, bind_data)).await? {
                                ast::Expr::Literal(lit) => {
                                    ExpressionContext::plan_literal(lit)?.try_into_scalar()?
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

    pub async fn bind_expressions(
        &self,
        exprs: impl IntoIterator<Item = ast::Expr<Raw>>,
        bind_data: &mut BindData,
    ) -> Result<Vec<ast::Expr<Bound>>> {
        let mut bound = Vec::new();
        for expr in exprs {
            bound.push(self.bind_expression(expr, bind_data).await?);
        }
        Ok(bound)
    }

    /// Bind an expression.
    pub async fn bind_expression(
        &self,
        expr: ast::Expr<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::Expr<Bound>> {
        match expr {
            ast::Expr::Ident(ident) => Ok(ast::Expr::Ident(ident)),
            ast::Expr::CompoundIdent(idents) => Ok(ast::Expr::CompoundIdent(idents)),
            ast::Expr::Literal(lit) => Ok(ast::Expr::Literal(match lit {
                ast::Literal::Number(s) => ast::Literal::Number(s),
                ast::Literal::SingleQuotedString(s) => ast::Literal::SingleQuotedString(s),
                ast::Literal::Boolean(b) => ast::Literal::Boolean(b),
                ast::Literal::Null => ast::Literal::Null,
                ast::Literal::Struct { keys, values } => {
                    let bound = Box::pin(self.bind_expressions(values, bind_data)).await?;
                    ast::Literal::Struct {
                        keys,
                        values: bound,
                    }
                }
            })),
            ast::Expr::Array(arr) => {
                let mut new_arr = Vec::with_capacity(arr.len());
                for v in arr {
                    let new_v = Box::pin(self.bind_expression(v, bind_data)).await?;
                    new_arr.push(new_v);
                }
                Ok(ast::Expr::Array(new_arr))
            }
            ast::Expr::ArraySubscript { expr, subscript } => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                let subscript = match *subscript {
                    ast::ArraySubscript::Index(index) => ast::ArraySubscript::Index(
                        Box::pin(self.bind_expression(index, bind_data)).await?,
                    ),
                    ast::ArraySubscript::Slice {
                        lower,
                        upper,
                        stride,
                    } => {
                        let lower = match lower {
                            Some(lower) => {
                                Some(Box::pin(self.bind_expression(lower, bind_data)).await?)
                            }
                            None => None,
                        };
                        let upper = match upper {
                            Some(upper) => {
                                Some(Box::pin(self.bind_expression(upper, bind_data)).await?)
                            }
                            None => None,
                        };
                        let stride = match stride {
                            Some(stride) => {
                                Some(Box::pin(self.bind_expression(stride, bind_data)).await?)
                            }
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
                        Box::pin(self.bind_expression(*expr, bind_data)).await
                    }
                    ast::UnaryOperator::Minus => match *expr {
                        ast::Expr::Literal(ast::Literal::Number(n)) => {
                            Ok(ast::Expr::Literal(ast::Literal::Number(format!("-{n}"))))
                        }
                        expr => Ok(ast::Expr::UnaryExpr {
                            op: UnaryOperator::Negate,
                            expr: Box::new(Box::pin(self.bind_expression(expr, bind_data)).await?),
                        }),
                    },
                    ast::UnaryOperator::Not => {
                        not_implemented!("bind not")
                    }
                }
            }
            ast::Expr::BinaryExpr { left, op, right } => Ok(ast::Expr::BinaryExpr {
                left: Box::new(Box::pin(self.bind_expression(*left, bind_data)).await?),
                op: op.try_into()?,
                right: Box::new(Box::pin(self.bind_expression(*right, bind_data)).await?),
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
                        Box::pin(self.bind_expression(*filter, bind_data)).await?,
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
                                    Box::pin(self.bind_expression(expr, bind_data)).await?,
                                ),
                            },
                        },
                        ast::FunctionArg::Unnamed { arg } => ast::FunctionArg::Unnamed {
                            arg: match arg {
                                ast::FunctionArgExpr::Wildcard => ast::FunctionArgExpr::Expr(
                                    ast::Expr::Literal(ast::Literal::Boolean(true)),
                                ),
                                ast::FunctionArgExpr::Expr(expr) => ast::FunctionArgExpr::Expr(
                                    Box::pin(self.bind_expression(expr, bind_data)).await?,
                                ),
                            },
                        },
                    };
                    args.push(func_arg);
                }

                // Check scalars first.
                if let Some(scalar) = self.binder.context.get_catalog(catalog)?.get_scalar_fn(
                    self.binder.tx,
                    schema,
                    func_name,
                )? {
                    // TODO: Allow unbound scalars?
                    // TODO: This also assumes scalars (and aggs) are the same everywhere, which
                    // they probably should be for now.
                    let bind_idx = bind_data
                        .functions
                        .push_bound(BoundFunction::Scalar(scalar), LocationRequirement::Any);
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: bind_idx,
                        args,
                        filter,
                    }));
                }

                // Now check aggregates.
                if let Some(aggregate) = self
                    .binder
                    .context
                    .get_catalog(catalog)?
                    .get_aggregate_fn(self.binder.tx, schema, func_name)?
                {
                    // TODO: Allow unbound aggregates?
                    let bind_idx = bind_data.functions.push_bound(
                        BoundFunction::Aggregate(aggregate),
                        LocationRequirement::Any,
                    );
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: bind_idx,
                        args,
                        filter,
                    }));
                }

                Err(RayexecError::new(format!(
                    "Cannot resolve function with name {}",
                    func.reference
                )))
            }
            ast::Expr::Subquery(subquery) => {
                let bound = Box::pin(self.binder.bind_query(*subquery, bind_data)).await?;
                Ok(ast::Expr::Subquery(Box::new(bound)))
            }
            ast::Expr::Exists {
                subquery,
                not_exists,
            } => {
                let bound = Box::pin(self.binder.bind_query(*subquery, bind_data)).await?;
                Ok(ast::Expr::Exists {
                    subquery: Box::new(bound),
                    not_exists,
                })
            }
            ast::Expr::TypedString { datatype, value } => {
                let datatype = Binder::ast_datatype_to_exec_datatype(datatype)?;
                Ok(ast::Expr::TypedString { datatype, value })
            }
            ast::Expr::Cast { datatype, expr } => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                let datatype = Binder::ast_datatype_to_exec_datatype(datatype)?;
                Ok(ast::Expr::Cast {
                    datatype,
                    expr: Box::new(expr),
                })
            }
            ast::Expr::Nested(expr) => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                Ok(ast::Expr::Nested(Box::new(expr)))
            }
            ast::Expr::Interval(ast::Interval {
                value,
                leading,
                trailing,
            }) => {
                let value = Box::pin(self.bind_expression(*value, bind_data)).await?;
                Ok(ast::Expr::Interval(ast::Interval {
                    value: Box::new(value),
                    leading,
                    trailing,
                }))
            }
            ast::Expr::Like {
                not_like,
                case_insensitive,
                expr,
                pattern,
            } => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                let pattern = Box::pin(self.bind_expression(*pattern, bind_data)).await?;
                Ok(ast::Expr::Like {
                    not_like,
                    case_insensitive,
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                })
            }
            other => not_implemented!("bind expr {other:?}"),
        }
    }
}
