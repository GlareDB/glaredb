use super::{
    expr::{ExpandedSelectExpr, ExpressionContext},
    operator::{
        Aggregate, AnyJoin, CrossJoin, GroupingExpr, Limit, LogicalExpression, LogicalOperator,
        Order, OrderByExpr, Projection,
    },
    scope::{ColumnRef, Scope},
    Resolver,
};
use crate::planner::{
    operator::{Explain, ExplainFormat, ExpressionList, Filter, JoinType, SetVar, ShowVar},
    scope::TableReference,
};
use rayexec_bullet::field::TypeSchema;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{
    ast::{self, OrderByNulls, OrderByType},
    statement::Statement,
};
use tracing::trace;

const EMPTY_SCOPE: &Scope = &Scope::empty();
const EMPTY_TYPE_SCHEMA: &TypeSchema = &TypeSchema::empty();

#[derive(Debug)]
pub struct LogicalQuery {
    /// Root of the query.
    pub root: LogicalOperator,

    /// The final scope of the query.
    pub scope: Scope,
}

#[derive(Debug, Clone)]
pub struct PlanContext<'a> {
    /// Resolver for resolving table and other table like items.
    pub resolver: &'a dyn Resolver,

    /// Scopes outside this context.
    pub outer_scopes: Vec<Scope>,
}

impl<'a> PlanContext<'a> {
    pub fn new(resolver: &'a dyn Resolver) -> Self {
        PlanContext {
            resolver,
            outer_scopes: Vec::new(),
        }
    }

    pub fn plan_statement(mut self, stmt: Statement) -> Result<LogicalQuery> {
        trace!("planning statement");
        match stmt {
            Statement::Explain(explain) => {
                let plan = match explain.body {
                    ast::ExplainBody::Query(query) => self.plan_query(query)?,
                };
                let format = match explain.output {
                    Some(ast::ExplainOutput::Text) => ExplainFormat::Text,
                    Some(ast::ExplainOutput::Json) => ExplainFormat::Json,
                    None => ExplainFormat::Text,
                };
                Ok(LogicalQuery {
                    root: LogicalOperator::Explain(Explain {
                        analyze: explain.analyze,
                        verbose: explain.verbose,
                        format,
                        input: Box::new(plan.root),
                    }),
                    scope: Scope::empty(),
                })
            }
            Statement::Query(query) => self.plan_query(query),
            Statement::SetVariable { reference, value } => {
                let expr_ctx = ExpressionContext::new(&self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
                let expr = expr_ctx.plan_expression(value)?;
                Ok(LogicalQuery {
                    root: LogicalOperator::SetVar(SetVar {
                        name: reference.0[0].value.clone(), // TODO: Normalize, allow compound references?
                        value: expr.try_into_scalar()?,
                    }),
                    scope: Scope::empty(),
                })
            }
            Statement::ShowVariable { reference } => {
                let name = reference.0[0].value.clone(); // TODO: Normalize, allow compound references?
                let var = self.resolver.get_session_variable(&name)?;
                let scope = Scope::with_columns(None, [name.clone()]);
                Ok(LogicalQuery {
                    root: LogicalOperator::ShowVar(ShowVar { var }),
                    scope,
                })
            }
            _ => unimplemented!(),
        }
    }

    /// Create a new nested plan context for planning subqueries.
    fn nested(&self, outer: Scope) -> Self {
        PlanContext {
            resolver: self.resolver,
            outer_scopes: std::iter::once(outer)
                .chain(self.outer_scopes.clone())
                .collect(),
        }
    }

    fn plan_query(&mut self, query: ast::QueryNode) -> Result<LogicalQuery> {
        // TODO: CTEs

        let mut planned = match query.body {
            ast::QueryNodeBody::Select(select) => {
                let mut plan = self.plan_select(*select)?;
                // TODO: I'd like to do this in plan select since it's allow for
                // reducing the number of expressions we're adding to the plan.
                //
                // Set expressions need this too, so some care needs to be taken
                // around that.
                if !query.order_by.is_empty() {
                    let input_schema = plan.root.output_schema(&[])?;
                    let expr_ctx = ExpressionContext::new(self, &plan.scope, &input_schema);

                    let mut exprs = Vec::with_capacity(query.order_by.len());
                    for order_by in query.order_by {
                        let expr = expr_ctx.plan_expression(order_by.expr)?;
                        let order_expr = OrderByExpr {
                            expr,
                            desc: matches!(
                                order_by.typ.unwrap_or(OrderByType::Desc),
                                OrderByType::Desc
                            ),
                            nulls_first: matches!(
                                order_by.nulls.unwrap_or(OrderByNulls::First),
                                OrderByNulls::First
                            ),
                        };

                        exprs.push(order_expr);
                    }

                    // Wrap plan in an order operator. Does not change scope.
                    plan.root = LogicalOperator::Order(Order {
                        exprs,
                        input: Box::new(plan.root),
                    })
                }

                plan
            }

            ast::QueryNodeBody::Set {
                left: _,
                right: _,
                operation: _,
            } => unimplemented!(),
            ast::QueryNodeBody::Values(values) => self.plan_values(values)?,
        };

        // Handle LIMIT/OFFSET
        let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
        if let Some(limit_expr) = query.limit.limit {
            let expr = expr_ctx.plan_expression(limit_expr)?;
            let limit = expr.try_into_scalar()?.try_as_i64()? as usize;

            let offset = match query.limit.offset {
                Some(offset_expr) => {
                    let expr = expr_ctx.plan_expression(offset_expr)?;
                    let offset = expr.try_into_scalar()?.try_as_i64()?;
                    Some(offset as usize)
                }
                None => None,
            };

            // Update plan, does not change scope.
            planned.root = LogicalOperator::Limit(Limit {
                offset,
                limit,
                input: Box::new(planned.root),
            });
        }

        Ok(planned)
    }

    fn plan_select(&mut self, select: ast::SelectNode) -> Result<LogicalQuery> {
        // Handle FROM
        let mut plan = match select.from {
            Some(from) => self.plan_from_node(from, Scope::empty())?,
            None => LogicalQuery {
                root: LogicalOperator::Empty,
                scope: Scope::empty(),
            },
        };

        let from_type_schema = plan.root.output_schema(&[])?;

        // Handle WHERE
        if let Some(where_expr) = select.where_expr {
            let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
            let expr = expr_ctx.plan_expression(where_expr)?;

            // Add filter to the plan, does not change the scope.
            plan.root = LogicalOperator::Filter(Filter {
                predicate: expr,
                input: Box::new(plan.root),
            });
        }

        // Expand projections.
        // TODO: Error on wildcards if no from.
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
        let mut projections = Vec::new();
        for select_proj in select.projections {
            let mut expanded = expr_ctx.expand_select_expr(select_proj)?;
            projections.append(&mut expanded);
        }

        // TODO:
        // - HAVING

        // Add projections to plan using previously expanded select items.
        let mut select_exprs = Vec::with_capacity(projections.len());
        let mut names = Vec::with_capacity(projections.len());
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
        for proj in projections {
            match proj {
                ExpandedSelectExpr::Expr { expr, name } => {
                    let expr = expr_ctx.plan_expression(expr)?;
                    select_exprs.push(expr);
                    names.push(name);
                }
                ExpandedSelectExpr::Column { idx, name } => {
                    let expr = LogicalExpression::ColumnRef(ColumnRef {
                        scope_level: 0,
                        item_idx: idx,
                    });
                    select_exprs.push(expr);
                    names.push(name);
                }
            }
        }

        // If we have any aggregates in the select, rewrite the plan to
        // accomadate the inputs into the aggregate, and add a projection for
        // the output of the aggregate.
        let has_aggregate = select_exprs.iter().any(|expr| expr.is_aggregate());
        if has_aggregate {
            let mut agg_exprs = Vec::new();
            let mut input_exprs = Vec::with_capacity(select_exprs.len());
            let mut final_column_indices = Vec::with_capacity(select_exprs.len());

            for (col_idx, expr) in select_exprs.into_iter().enumerate() {
                match expr {
                    LogicalExpression::Aggregate {
                        agg,
                        mut inputs,
                        filter,
                    } => {
                        // Need to push the inputs to the aggregates to
                        // `input_exprs` and rewrite aggregate to use those.
                        let agg_input_start = input_exprs.len();
                        let agg_input_count = inputs.len();

                        // TODO: Need to check that the user isn't trying to
                        // nest aggregate expressions.
                        input_exprs.append(&mut inputs);

                        // Compute new column exprs.
                        let new_inputs: Vec<_> = (agg_input_start
                            ..(agg_input_start + agg_input_count))
                            .map(|col| {
                                LogicalExpression::ColumnRef(ColumnRef {
                                    scope_level: 0,
                                    item_idx: col,
                                })
                            })
                            .collect();

                        let new_agg = LogicalExpression::Aggregate {
                            agg,
                            inputs: new_inputs,
                            filter,
                        };

                        agg_exprs.push(new_agg);
                        final_column_indices.push(col_idx);
                    }
                    other => {
                        // No need to rewrite this expression, just make sure
                        // the final projection maps to the correct column in
                        // the input projection.
                        let mapped_idx = input_exprs.len();
                        input_exprs.push(other);
                        agg_exprs.push(LogicalExpression::ColumnRef(ColumnRef {
                            scope_level: 0,
                            item_idx: mapped_idx,
                        }));
                        final_column_indices.push(col_idx);
                    }
                }
            }

            // Get the expressions in the group by.
            let mut grouping_expr = match select.group_by {
                Some(group_by) => {
                    match group_by {
                        ast::GroupByNode::All => unimplemented!(),
                        ast::GroupByNode::Exprs { mut exprs } => {
                            if exprs.len() != 1 {
                                // TODO: Support this.
                                return Err(RayexecError::new(
                                    "multiple expressions in GROUP BY not supported",
                                ));
                            }
                            let expr = exprs.pop().unwrap();

                            // What's in scope for the plan is in scope for the
                            // group by.
                            let expr_ctx =
                                ExpressionContext::new(self, &plan.scope, &from_type_schema);

                            match expr {
                                ast::GroupByExpr::Expr(exprs) => {
                                    let exprs = exprs
                                        .into_iter()
                                        .map(|expr| expr_ctx.plan_expression(expr))
                                        .collect::<Result<Vec<_>>>()?;
                                    GroupingExpr::GroupBy(exprs)
                                }
                                ast::GroupByExpr::Rollup(exprs) => {
                                    let exprs = exprs
                                        .into_iter()
                                        .map(|expr| expr_ctx.plan_expression(expr))
                                        .collect::<Result<Vec<_>>>()?;
                                    GroupingExpr::Rollup(exprs)
                                }
                                _ => unimplemented!(),
                            }
                        }
                    }
                }
                None => GroupingExpr::None,
            };

            // Now we iterate over the expressions in the group by and make sure
            // to include them in the pre-projection. The group by expressions
            // will then be modified to point to the output of this projection.
            for group_by_expr in grouping_expr.expressions_mut().iter_mut() {
                // TODO: This currently just moves all expressions into the
                // pre-projection. We could be smart here and instead check if
                // this expression is already in the pre-projection and just
                // point to that.
                //
                // For example:
                //
                // SELECT column1, SUM(column2) FROM table GROUP BY colum1;
                //
                // Will end up with [column1, column2, column1] in the
                // pre-projection. For basic columns, this is fine since they're
                // just behind an Arc, and so it'll just be cheaply cloned,
                // however if there's actual computation in the expression (e.g.
                // column1 / 100 is both in the select and group by), we'll end
                // up computing that twice.
                let col_idx = input_exprs.len();
                let replacement_expr = LogicalExpression::ColumnRef(ColumnRef {
                    scope_level: 0,
                    item_idx: col_idx,
                });
                let actual_expr = std::mem::replace(group_by_expr, replacement_expr);
                input_exprs.push(actual_expr);
            }

            // Apply input projection.
            //
            // This projection contains any columns used as inputs into
            // aggregate functions _and_ columns used in a GROUP BY.
            let input_plan = LogicalOperator::Projection(Projection {
                exprs: input_exprs,
                input: Box::new(plan.root),
            });

            // Generate the aggregate plan.
            let agg_plan = LogicalOperator::Aggregate(Aggregate {
                exprs: agg_exprs,
                grouping_expr,
                input: Box::new(input_plan),
            });

            // Apply a final projection omitting inputs to the aggreate, and any
            // columns/expressions we needed for the group by.
            //
            // These contain only column references since any computation should
            // have happened on the input to the aggregate node.
            let output_cols: Vec<_> = final_column_indices
                .into_iter()
                .map(|col| {
                    LogicalExpression::ColumnRef(ColumnRef {
                        scope_level: 0,
                        item_idx: col,
                    })
                })
                .collect();

            plan = LogicalQuery {
                root: LogicalOperator::Projection(Projection {
                    exprs: output_cols,
                    input: Box::new(agg_plan),
                }),
                scope: Scope::with_columns(None, names),
            }
        } else {
            // No aggregates, we can just use the select expressions directly.

            // TODO: Check group by, make sure it doesn't exist.

            plan = LogicalQuery {
                root: LogicalOperator::Projection(Projection {
                    exprs: select_exprs,
                    input: Box::new(plan.root),
                }),
                // Cleaned scope containing only output columns in the
                // projection.
                scope: Scope::with_columns(None, names),
            };
        }

        Ok(plan)
    }

    fn plan_from_node(&self, from: ast::FromNode, current_scope: Scope) -> Result<LogicalQuery> {
        // Plan the "body" of the FROM.
        let body = match from.body {
            ast::FromNodeBody::BaseTable(_) => unimplemented!(),
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                let mut nested = self.nested(current_scope);
                nested.plan_query(query)?
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                reference: _,
                args: _,
            }) => {
                unimplemented!()
                // let func = self.resolver.resolve_table_function(&reference)?;

                // // Plan the arguments to the table function. Currently only
                // // constant expressions are allowed.
                // let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
                // let mut func_args = TableFunctionArgs::default();
                // for arg in args {
                //     match arg {
                //         ast::FunctionArg::Named { name, arg } => {
                //             match expr_ctx.plan_expression(arg)? {
                //                 LogicalExpression::Literal(v) => {
                //                     func_args.named.insert(name.value, v);
                //                 }
                //                 other => {
                //                     return Err(RayexecError::new(format!(
                //                         "Argument to table funtion is not a constant: {other:?}"
                //                     )))
                //                 }
                //             }
                //         }
                //         ast::FunctionArg::Unnamed { arg } => {
                //             match expr_ctx.plan_expression(arg)? {
                //                 LogicalExpression::Literal(v) => func_args.unnamed.push(v),
                //                 other => {
                //                     return Err(RayexecError::new(format!(
                //                         "Argument to table funtion is not a constant: {other:?}"
                //                     )))
                //                 }
                //             }
                //         }
                //     }
                // }

                // let name = func.name();
                // let bound = func.bind(func_args)?; // The only thing that would benefit from async.
                // let schema = bound.schema().clone();

                // // Create a new scope with just this table function.
                // // TODO: Reference should probably be qualified.
                // let scope = Scope::with_columns(
                //     Some(TableReference {
                //         database: None,
                //         schema: None,
                //         table: name.to_string(),
                //     }),
                //     schema.iter().map(|field| field.name.clone()),
                // );

                // let operator = LogicalOperator::Scan(Scan {
                //     source: ScanItem::TableFunction(bound),
                //     schema: schema.into_type_schema(),
                // });

                // LogicalQuery {
                //     root: operator,
                //     scope,
                // }
            }
            ast::FromNodeBody::Join(ast::FromJoin {
                left,
                right,
                join_type,
                join_condition,
            }) => {
                // Plan left side of join.
                let left_nested = self.nested(current_scope.clone());
                let left_plan = left_nested.plan_from_node(*left, Scope::empty())?; // TODO: Determine if should be empty.

                // Plan right side of join.
                //
                // Note this uses a plan context that has the "left" scope as
                // its outer scope.
                let right_nested = left_nested.nested(left_plan.scope.clone());
                let right_plan = right_nested.plan_from_node(*right, Scope::empty())?; // TODO: Determine if this should be empty.

                match join_condition {
                    ast::JoinCondition::On(on) => {
                        let merged = left_plan.scope.merge(right_plan.scope)?;
                        let left_schema = left_plan.root.output_schema(&[])?; // TODO: Outers
                        let right_schema = right_plan.root.output_schema(&[])?; // TODO: Outers
                        let merged_schema = left_schema.merge(right_schema);
                        let expr_ctx =
                            ExpressionContext::new(&left_nested, &merged, &merged_schema);

                        let on_expr = expr_ctx.plan_expression(on)?;

                        let join_type = match join_type {
                            ast::JoinType::Inner => JoinType::Inner,
                            ast::JoinType::Left => JoinType::Left,
                            ast::JoinType::Right => JoinType::Right,
                            ast::JoinType::Cross => {
                                unreachable!("Cross join should not have a join condition")
                            }
                            _ => unimplemented!(),
                        };

                        LogicalQuery {
                            root: LogicalOperator::AnyJoin(AnyJoin {
                                left: Box::new(left_plan.root),
                                right: Box::new(right_plan.root),
                                join_type,
                                on: on_expr,
                            }),
                            scope: merged,
                        }
                    }
                    ast::JoinCondition::None => match join_type {
                        ast::JoinType::Cross => {
                            let merged = left_plan.scope.merge(right_plan.scope)?;
                            LogicalQuery {
                                root: LogicalOperator::CrossJoin(CrossJoin {
                                    left: Box::new(left_plan.root),
                                    right: Box::new(right_plan.root),
                                }),
                                scope: merged,
                            }
                        }
                        _other => return Err(RayexecError::new("Missing join condition for join")),
                    },
                    _ => unimplemented!(),
                }
            }
        };

        // Apply aliases if provided.
        let aliased_scope = Self::apply_alias(body.scope, from.alias)?;

        Ok(LogicalQuery {
            root: body.root,
            scope: aliased_scope,
        })
    }

    /// Apply table and column aliases to a scope.
    fn apply_alias(mut scope: Scope, alias: Option<ast::FromAlias>) -> Result<Scope> {
        Ok(match alias {
            Some(ast::FromAlias { alias, columns }) => {
                let reference = TableReference {
                    database: None,
                    schema: None,
                    table: alias.value,
                };

                // Modify all items in the scope to now have the new table
                // alias.
                for item in scope.items.iter_mut() {
                    // TODO: Make sure that it's correct to apply this to
                    // everything in the scope.
                    item.alias = Some(reference.clone());
                }

                // If column aliases are provided as well, apply those to the
                // columns in the scope.
                //
                // Note that if the user supplies less aliases than there are
                // columns in the scope, then the remaining columns will retain
                // their original names.
                if let Some(columns) = columns {
                    if columns.len() > scope.items.len() {
                        return Err(RayexecError::new(format!(
                            "Specified {} column aliases when only {} columns exist",
                            columns.len(),
                            scope.items.len()
                        )));
                    }

                    for (item, new_alias) in scope.items.iter_mut().zip(columns.into_iter()) {
                        item.column = new_alias.value;
                    }
                }

                scope
            }
            None => scope,
        })
    }

    fn plan_values(&self, values: ast::Values) -> Result<LogicalQuery> {
        if values.rows.is_empty() {
            return Err(RayexecError::new("Empty VALUES expression"));
        }

        // Convert AST expressions to logical expressions.
        let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
        let num_cols = values.rows[0].len();
        let exprs = values
            .rows
            .into_iter()
            .map(|col_vals| {
                col_vals
                    .into_iter()
                    .map(|col_expr| expr_ctx.plan_expression(col_expr))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<Vec<LogicalExpression>>>>()?;

        let operator = LogicalOperator::ExpressionList(ExpressionList { rows: exprs });

        // Generate output scope with appropriate column names.
        let mut scope = Scope::empty();
        scope.add_columns(None, (0..num_cols).map(|i| format!("column{}", i + 1)));

        Ok(LogicalQuery {
            root: operator,
            scope,
        })
    }
}
