// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::str::FromStr;

use datafusion::common::{
    not_impl_err,
    plan_datafusion_err,
    plan_err,
    DFSchema,
    DataFusionError,
    Dependency,
    Result,
};
use datafusion::logical_expr::expr::{find_df_window_func, ScalarFunction};
use datafusion::logical_expr::function::suggest_valid_function;
use datafusion::logical_expr::window_frame::{check_window_frame, regularize_window_order_by};
use datafusion::logical_expr::{
    expr,
    AggregateFunction,
    BuiltinScalarFunction,
    Expr,
    WindowFrame,
    WindowFunctionDefinition,
};
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SQLExpr,
    Function as SQLFunction,
    FunctionArg,
    FunctionArgExpr,
    WindowType,
};

use super::arrow_cast::ARROW_CAST_NAME;
use crate::planner::expr::arrow_cast::create_arrow_cast;
use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    pub(super) async fn sql_function_to_expr(
        &mut self,
        function: SQLFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let SQLFunction {
            name,
            args,
            over,
            distinct,
            filter,
            null_treatment,
            special: _, // true if not called with trailing parens
            order_by,
        } = function;

        if let Some(null_treatment) = null_treatment {
            return not_impl_err!(
                "Null treatment in aggregate functions is not supported: {null_treatment}"
            );
        }

        let name = if name.0.len() > 1 {
            // DF doesn't handle compound identifiers
            // (e.g. "foo.bar") for function names yet
            name.to_string()
        } else {
            crate::planner::utils::normalize_ident(name.0[0].clone())
        };

        let args = self
            .function_args_to_expr(args, schema, planner_context)
            .await?;

        // user-defined function (UDF) should have precedence in case it has the same name as a scalar built-in function
        if let Some(expr) = self
            .context_provider
            .get_function_meta(&name, &args)
            .await?
        {
            return Ok(expr);
        }

        // next, scalar built-in
        if let Ok(fun) = BuiltinScalarFunction::from_str(&name) {
            return Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)));
        };

        // If function is a window function (it has an OVER clause),
        // it shouldn't have ordering requirement as function argument
        // required ordering should be defined in OVER clause.
        let is_function_window = over.is_some();
        if !order_by.is_empty() && is_function_window {
            return plan_err!("Aggregate ORDER BY is not implemented for window functions");
        }

        // then, window function
        if let Some(WindowType::WindowSpec(window)) = over {
            let partition_by = {
                let mut partition_by = Vec::with_capacity(window.partition_by.len());
                for e in window.partition_by {
                    let e = self
                        .sql_expr_to_logical_expr(e, schema, planner_context)
                        .await?;
                    partition_by.push(e);
                }
                partition_by
            };

            let mut order_by = self
                .order_by_to_sort_expr(
                    &window.order_by,
                    schema,
                    planner_context,
                    // Numeric literals in window function ORDER BY are treated as constants
                    false,
                )
                .await?;

            let func_deps = schema.functional_dependencies();
            // Find whether ties are possible in the given ordering:
            let is_ordering_strict = order_by.iter().any(|orderby_expr| {
                if let Expr::Sort(sort_expr) = orderby_expr {
                    if let Expr::Column(col) = sort_expr.expr.as_ref() {
                        let idx = schema.index_of_column(col).unwrap();
                        return func_deps.iter().any(|dep| {
                            dep.source_indices == vec![idx] && dep.mode == Dependency::Single
                        });
                    }
                }
                false
            });

            let window_frame = window
                .window_frame
                .as_ref()
                .map(|window_frame| {
                    let window_frame = window_frame.clone().try_into()?;
                    check_window_frame(&window_frame, order_by.len()).map(|_| window_frame)
                })
                .transpose()?;

            let window_frame = if let Some(window_frame) = window_frame {
                regularize_window_order_by(&window_frame, &mut order_by)?;
                window_frame
            } else if is_ordering_strict {
                WindowFrame::new(Some(true))
            } else {
                WindowFrame::new((!order_by.is_empty()).then_some(false))
            };

            if let Ok(fun) = self.find_window_func(&name).await {
                let expr = match fun {
                    WindowFunctionDefinition::AggregateFunction(aggregate_fun) => {
                        Expr::WindowFunction(expr::WindowFunction::new(
                            WindowFunctionDefinition::AggregateFunction(aggregate_fun),
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                        ))
                    }
                    _ => Expr::WindowFunction(expr::WindowFunction::new(
                        fun,
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                    )),
                };
                return Ok(expr);
            }
        } else {
            // User defined aggregate functions (UDAF) have precedence in case it has the same name as a scalar built-in function
            if let Some(fm) = self.context_provider.get_aggregate_meta(&name).await {
                return Ok(Expr::AggregateFunction(expr::AggregateFunction::new_udf(
                    fm, args, false, None, None,
                )));
            }

            // next, aggregate built-ins
            if let Ok(fun) = AggregateFunction::from_str(&name) {
                let order_by = self
                    .order_by_to_sort_expr(&order_by, schema, planner_context, true)
                    .await?;
                let order_by = (!order_by.is_empty()).then_some(order_by);

                let filter: Option<Box<Expr>> = match filter {
                    Some(e) => {
                        let e = self
                            .sql_expr_to_logical_expr(*e, schema, planner_context)
                            .await?;
                        Some(Box::new(e))
                    }
                    None => None,
                };

                return Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                    fun, args, distinct, filter, order_by,
                )));
            };

            // Special case arrow_cast (as its type is dependent on its argument value)
            if name == ARROW_CAST_NAME {
                return create_arrow_cast(args, schema);
            }
        }

        // Could not find the relevant function, so return an error
        let suggested_func_name = suggest_valid_function(&name, is_function_window);
        plan_err!("Invalid function '{name}'.\nDid you mean '{suggested_func_name}'?")
    }

    pub(super) async fn sql_named_function_to_expr(
        &mut self,
        expr: SQLExpr,
        fun: BuiltinScalarFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args = vec![
            self.sql_expr_to_logical_expr(expr, schema, planner_context)
                .await?,
        ];
        Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)))
    }

    pub(super) async fn find_window_func(
        &mut self,
        name: &str,
    ) -> Result<WindowFunctionDefinition> {
        if let Some(func) = find_df_window_func(name) {
            return Ok(func);
        }

        if let Some(agg) = self.context_provider.get_aggregate_meta(name).await {
            return Ok(expr::WindowFunctionDefinition::AggregateUDF(agg));
        }

        if let Some(win) = self.context_provider.get_window_meta(name).await {
            return Ok(WindowFunctionDefinition::WindowUDF(win));
        }

        Err(plan_datafusion_err!(
            "There is no window function named {name}"
        ))
    }

    async fn sql_fn_arg_to_logical_expr(
        &mut self,
        sql: FunctionArg,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match sql {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => {
                self.sql_expr_to_logical_expr(arg, schema, planner_context)
                    .await
            }
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Ok(Expr::Wildcard { qualifier: None }),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.sql_expr_to_logical_expr(arg, schema, planner_context)
                    .await
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Ok(Expr::Wildcard { qualifier: None })
            }
            _ => not_impl_err!("Unsupported qualified wildcard argument: {sql:?}"),
        }
    }

    pub(super) async fn function_args_to_expr(
        &mut self,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        let mut exprs = Vec::with_capacity(args.len());
        for a in args {
            let e = self
                .sql_fn_arg_to_logical_expr(a, schema, planner_context)
                .await?;
            exprs.push(e);
        }
        Ok(exprs)
    }
}
