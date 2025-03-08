use std::collections::HashMap;

use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast::{self, FunctionArg, ReplaceColumn};
use rayexec_parser::meta::Raw;

use super::resolve_normal::create_user_facing_resolve_err;
use super::resolved_function::{ResolvedFunction, SpecialBuiltinFunction};
use super::resolved_table_function::ConstantFunctionArgs;
use super::{ResolveContext, ResolvedMeta, Resolver};
use crate::catalog::entry::CatalogEntryType;
use crate::catalog::{Catalog, Schema};
use crate::logical::binder::expr_binder::BaseExpressionBinder;
use crate::logical::operator::LocationRequirement;

#[derive(Debug)]
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

    /// Resolves constant function arguments for a table function.
    ///
    /// Slightly different from normal argument resolving since arguments to a
    /// table function are more restrictive. E.g. we only allow literals as
    /// arguments for scan table functions.
    pub async fn resolve_constant_table_function_args(
        &self,
        args: Vec<FunctionArg<Raw>>,
    ) -> Result<ConstantFunctionArgs> {
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

        Ok(ConstantFunctionArgs { named, positional })
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

    pub async fn resolve_optional_expression(
        &self,
        expr: Option<ast::Expr<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<Option<ast::Expr<ResolvedMeta>>> {
        Ok(match expr {
            Some(expr) => {
                let expr = Box::pin(self.resolve_expression(expr, resolve_context)).await?;
                Some(expr)
            }
            None => None,
        })
    }

    /// Resolve an expression.
    pub async fn resolve_expression(
        &self,
        expr: ast::Expr<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
        // Match arms that produce a bunch of intermediate variables should be
        // split out into a separate function.
        //
        // This is necessary since compiling in debug mode with no optimizations
        // would produce very large stack frames for this function. And since
        // it's recursively called, it's very easy to hit a stack overflow. By
        // splitting out some arms into separate functions, we avoid having
        // those variables count towards stack usage.
        //
        // This is not a problem with optimizations (at least O1). I just don't
        // want to have to do that for our unit/integration tests if I don't
        // have to.
        //
        // See <https://github.com/rust-lang/rust/issues/34283>
        // check_stack_redline("resolve expression")?;

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
            ast::Expr::Array(arr) => self.resolve_array(arr, resolve_context).await,
            ast::Expr::ArraySubscript { expr, subscript } => {
                self.resolve_array_subscript(expr, subscript, resolve_context)
                    .await
            }
            ast::Expr::UnaryExpr { op, expr } => {
                self.resolve_unary_expr(op, expr, resolve_context).await
            }
            ast::Expr::BinaryExpr { left, op, right } => Ok(ast::Expr::BinaryExpr {
                left: Box::new(Box::pin(self.resolve_expression(*left, resolve_context)).await?),
                op,
                right: Box::new(Box::pin(self.resolve_expression(*right, resolve_context)).await?),
            }),
            ast::Expr::Function(func) => {
                self.resolve_scalar_or_aggregate_function(func, resolve_context)
                    .await
            }
            ast::Expr::Subquery(subquery) => self.resolve_subquery(subquery, resolve_context).await,
            ast::Expr::Exists {
                subquery,
                not_exists,
            } => {
                self.resolve_exists_subquery(subquery, not_exists, resolve_context)
                    .await
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
                self.resolve_case(expr, conditions, results, else_expr, resolve_context)
                    .await
            }
            ast::Expr::Substring { expr, from, count } => {
                self.resolve_substring(expr, from, count, resolve_context)
                    .await
            }
            ast::Expr::Extract { date_part, expr } => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                Ok(ast::Expr::Extract {
                    date_part,
                    expr: Box::new(expr),
                })
            }
            ast::Expr::Columns(col) => Ok(ast::Expr::Columns(col)),
            other => not_implemented!("resolve expr {other:?}"),
        }
    }

    async fn resolve_subquery(
        &self,
        subquery: Box<ast::QueryNode<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
        let resolved = Box::pin(self.resolver.resolve_query(*subquery, resolve_context)).await?;
        Ok(ast::Expr::Subquery(Box::new(resolved)))
    }

    async fn resolve_exists_subquery(
        &self,
        subquery: Box<ast::QueryNode<Raw>>,
        not_exists: bool,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
        let resolved = Box::pin(self.resolver.resolve_query(*subquery, resolve_context)).await?;
        Ok(ast::Expr::Exists {
            subquery: Box::new(resolved),
            not_exists,
        })
    }

    async fn resolve_substring(
        &self,
        expr: Box<ast::Expr<Raw>>,
        from: Box<ast::Expr<Raw>>,
        count: Option<Box<ast::Expr<Raw>>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
        let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
        let from = Box::pin(self.resolve_expression(*from, resolve_context)).await?;
        let count = match count {
            Some(count) => Some(Box::pin(self.resolve_expression(*count, resolve_context)).await?),
            None => None,
        };

        Ok(ast::Expr::Substring {
            expr: Box::new(expr),
            from: Box::new(from),
            count: count.map(Box::new),
        })
    }

    async fn resolve_case(
        &self,
        expr: Option<Box<ast::Expr<Raw>>>,
        conditions: Vec<ast::Expr<Raw>>,
        results: Vec<ast::Expr<Raw>>,
        else_expr: Option<Box<ast::Expr<Raw>>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
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
        let conditions = Box::pin(self.resolve_expressions(conditions, resolve_context)).await?;
        let results = Box::pin(self.resolve_expressions(results, resolve_context)).await?;

        Ok(ast::Expr::Case {
            expr,
            conditions,
            results,
            else_expr,
        })
    }

    async fn resolve_array(
        &self,
        arr: Vec<ast::Expr<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
        let mut new_arr = Vec::with_capacity(arr.len());
        for v in arr {
            let new_v = Box::pin(self.resolve_expression(v, resolve_context)).await?;
            new_arr.push(new_v);
        }
        Ok(ast::Expr::Array(new_arr))
    }

    async fn resolve_array_subscript(
        &self,
        expr: Box<ast::Expr<Raw>>,
        subscript: Box<ast::ArraySubscript<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
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
                    Some(lower) => {
                        Some(Box::pin(self.resolve_expression(lower, resolve_context)).await?)
                    }
                    None => None,
                };
                let upper = match upper {
                    Some(upper) => {
                        Some(Box::pin(self.resolve_expression(upper, resolve_context)).await?)
                    }
                    None => None,
                };
                let stride = match stride {
                    Some(stride) => {
                        Some(Box::pin(self.resolve_expression(stride, resolve_context)).await?)
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

    async fn resolve_unary_expr(
        &self,
        op: ast::UnaryOperator,
        expr: Box<ast::Expr<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
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
                    expr: Box::new(Box::pin(self.resolve_expression(expr, resolve_context)).await?),
                }),
            },
            ast::UnaryOperator::Not => {
                let expr = Box::pin(self.resolve_expression(*expr, resolve_context)).await?;
                Ok(ast::Expr::UnaryExpr {
                    op,
                    expr: Box::new(expr),
                })
            }
        }
    }

    async fn resolve_scalar_or_aggregate_function(
        &self,
        mut func: Box<ast::Function<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Expr<ResolvedMeta>> {
        // TODO: Search path (with system being the first to check)
        let (catalog, schema, func_name) = match func.reference.0.len() {
            0 => return Err(RayexecError::new("Missing idents for function reference")), // Shouldn't happen.
            1 => (
                "system".to_string(),
                "glare_catalog".to_string(),
                func.reference.0[0].as_normalized_string(),
            ),
            2 => (
                "system".to_string(),
                func.reference.0[0].as_normalized_string(),
                func.reference.0[1].as_normalized_string(),
            ),
            3 => (
                func.reference.0[0].as_normalized_string(),
                func.reference.0[1].as_normalized_string(),
                func.reference.0[2].as_normalized_string(),
            ),
            _ => {
                // TODO: This could technically be from chained syntax on a
                // fully qualified column.
                return Err(RayexecError::new("Too many idents for function reference")
                    .with_field("idents", func.reference.to_string()));
            }
        };

        let context = self.resolver.context;

        // See if we can resolve the catalog & schema. If we can't assume we're
        // using chained function syntax.
        //
        // TODO: Make `get_database` return Option.
        // TODO: We should be exhaustive about what's part of the qualified
        //       function call vs what's part of the column.
        let is_qualified = func.reference.0.len() > 1;
        if self.resolver.config.enable_function_chaining
            && is_qualified
            && (!context.get_database(&catalog).is_some()
                || context
                    .require_get_database(&catalog)?
                    .catalog
                    .get_schema(&schema)?
                    .is_none())
        {
            let unqualified_name = func.reference.0.pop().unwrap(); // Length checked above.
            let unqualified_ref = ast::ObjectReference(vec![unqualified_name]);

            let mut prefix_ref = std::mem::replace(&mut func.reference, unqualified_ref);

            // Now add the prefix we took from the reference as the first
            // argument to the function.

            // TODO: Expr binder should probably take of this for us.
            let arg_expr = match prefix_ref.0.len() {
                1 => ast::Expr::Ident(prefix_ref.0.pop().unwrap()),
                _ => ast::Expr::CompoundIdent(prefix_ref.0),
            };

            func.args.insert(
                0,
                ast::FunctionArg::Unnamed {
                    arg: ast::FunctionArgExpr::Expr(arg_expr),
                },
            );

            // Now try to resolve with just the unqualified reference.
            let resolved =
                Box::pin(self.resolve_scalar_or_aggregate_function(func, resolve_context)).await?;

            return Ok(resolved);
        }

        let filter = self
            .resolve_optional_expression(func.filter.map(|e| *e), resolve_context)
            .await?
            .map(Box::new);

        let over = match func.over {
            Some(over) => Some(self.resolve_window_spec(over, resolve_context).await?),
            None => None,
        };
        let args = Box::pin(self.resolve_function_args(func.args, resolve_context)).await?;

        let schema_ent = context
            .require_get_database(&catalog)?
            .catalog
            .get_schema(&schema)?
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;

        // Check if this is a special function.
        if let Some(special) = SpecialBuiltinFunction::try_from_name(&func_name) {
            let resolve_idx = resolve_context
                .functions
                .push_resolved(ResolvedFunction::Special(special), LocationRequirement::Any);

            return Ok(ast::Expr::Function(Box::new(ast::Function {
                reference: resolve_idx,
                distinct: func.distinct,
                args,
                filter,
                over,
            })));
        }

        // Now check scalars.
        if let Some(scalar) = schema_ent.get_scalar_function(&func_name)? {
            // TODO: Allow unresolved scalars?
            // TODO: This also assumes scalars (and aggs) are the same everywhere, which
            // they probably should be for now.
            let resolve_idx = resolve_context.functions.push_resolved(
                ResolvedFunction::Scalar(scalar.try_as_scalar_function_entry()?.function.clone()),
                LocationRequirement::Any,
            );
            return Ok(ast::Expr::Function(Box::new(ast::Function {
                reference: resolve_idx,
                distinct: func.distinct,
                args,
                filter,
                over,
            })));
        }

        // Now check aggregates.
        if let Some(aggregate) = schema_ent.get_aggregate_function(&func_name)? {
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
            return Ok(ast::Expr::Function(Box::new(ast::Function {
                reference: resolve_idx,
                distinct: func.distinct,
                args,
                filter,
                over,
            })));
        }

        Err(create_user_facing_resolve_err(
            Some(&schema_ent),
            &[
                CatalogEntryType::ScalarFunction,
                CatalogEntryType::AggregateFunction,
            ],
            &func_name,
        ))
    }

    pub(crate) async fn resolve_function_args(
        &self,
        args: Vec<ast::FunctionArg<Raw>>,
        resolve_context: &mut ResolveContext,
    ) -> Result<Vec<ast::FunctionArg<ResolvedMeta>>> {
        let mut resolved_args = Vec::with_capacity(args.len());
        // TODO: This current rewrites '*' function arguments to 'true'.
        // This is for 'count(*)'. What we should be doing is rewriting
        // 'count(*)' to 'count_star()' and have a function
        // implementation for 'count_star'.
        //
        // No other function accepts a '*' (I think).
        for func_arg in args {
            let func_arg = match func_arg {
                ast::FunctionArg::Named { name, arg } => ast::FunctionArg::Named {
                    name,
                    arg: match arg {
                        ast::FunctionArgExpr::Wildcard => ast::FunctionArgExpr::Expr(
                            ast::Expr::Literal(ast::Literal::Boolean(true)),
                        ),
                        ast::FunctionArgExpr::Expr(expr) => ast::FunctionArgExpr::Expr(
                            Box::pin(self.resolve_expression(expr, resolve_context)).await?,
                        ),
                    },
                },
                ast::FunctionArg::Unnamed { arg } => ast::FunctionArg::Unnamed {
                    arg: match arg {
                        ast::FunctionArgExpr::Wildcard => ast::FunctionArgExpr::Expr(
                            ast::Expr::Literal(ast::Literal::Boolean(true)),
                        ),
                        ast::FunctionArgExpr::Expr(expr) => ast::FunctionArgExpr::Expr(
                            Box::pin(self.resolve_expression(expr, resolve_context)).await?,
                        ),
                    },
                },
            };
            resolved_args.push(func_arg);
        }

        Ok(resolved_args)
    }

    /// Handle resolving the `OVER (...)` clause for a window function.
    async fn resolve_window_spec(
        &self,
        over: ast::WindowSpec<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::WindowSpec<ResolvedMeta>> {
        async fn resolve_window_frame_bound(
            resolver: &ExpressionResolver<'_>,
            bound: ast::WindowFrameBound<Raw>,
            resolve_context: &mut ResolveContext,
        ) -> Result<ast::WindowFrameBound<ResolvedMeta>> {
            Ok(match bound {
                ast::WindowFrameBound::CurrentRow => ast::WindowFrameBound::CurrentRow,
                ast::WindowFrameBound::UnboundedPreceding => {
                    ast::WindowFrameBound::UnboundedPreceding
                }
                ast::WindowFrameBound::UnboundedFollowing => {
                    ast::WindowFrameBound::UnboundedFollowing
                }
                ast::WindowFrameBound::Preceding(expr) => {
                    let expr =
                        Box::pin(resolver.resolve_expression(*expr, resolve_context)).await?;
                    ast::WindowFrameBound::Preceding(Box::new(expr))
                }
                ast::WindowFrameBound::Following(expr) => {
                    let expr =
                        Box::pin(resolver.resolve_expression(*expr, resolve_context)).await?;
                    ast::WindowFrameBound::Following(Box::new(expr))
                }
            })
        }

        match over {
            ast::WindowSpec::Definition(window_def) => {
                let partition_by =
                    Box::pin(self.resolve_expressions(window_def.partition_by, resolve_context))
                        .await?;

                let mut order_by = Vec::with_capacity(window_def.order_by.len());
                for order in window_def.order_by {
                    let order = ast::OrderByNode {
                        typ: order.typ,
                        nulls: order.nulls,
                        expr: Box::pin(self.resolve_expression(order.expr, resolve_context))
                            .await?,
                    };

                    order_by.push(order);
                }

                let frame = match window_def.frame {
                    Some(frame) => {
                        let start =
                            resolve_window_frame_bound(self, frame.start, resolve_context).await?;
                        let end = match frame.end {
                            Some(end) => {
                                Some(resolve_window_frame_bound(self, end, resolve_context).await?)
                            }
                            None => None,
                        };

                        Some(ast::WindowFrame {
                            unit: frame.unit,
                            start,
                            end,
                            exclusion: frame.exclusion,
                        })
                    }
                    None => None,
                };

                Ok(ast::WindowSpec::Definition(ast::WindowDefinition {
                    existing: window_def.existing,
                    partition_by,
                    order_by,
                    frame,
                }))
            }
            ast::WindowSpec::Named(ident) => Ok(ast::WindowSpec::Named(ident)),
        }
    }
}
