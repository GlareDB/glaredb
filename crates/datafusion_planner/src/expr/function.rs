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

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};
use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion::logical_expr::expr::{ScalarFunction, ScalarUDF};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::window_frame::regularize;
use datafusion::logical_expr::{
    expr, window_function, AggregateFunction, BuiltinScalarFunction, Expr, WindowFrame,
    WindowFunction,
};
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SQLExpr, Function as SQLFunction, FunctionArg, FunctionArgExpr, WindowType,
};
use std::str::FromStr;

use super::arrow_cast::ARROW_CAST_NAME;

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    pub(super) async fn sql_function_to_expr(
        &mut self,
        mut function: SQLFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let name = if function.name.0.len() > 1 {
            // DF doesn't handle compound identifiers
            // (e.g. "foo.bar") for function names yet
            function.name.to_string()
        } else {
            self.normalizer.normalize(function.name.0[0].clone())
        };

        // next, scalar built-in
        if let Ok(fun) = BuiltinScalarFunction::from_str(&name) {
            let args = self
                .function_args_to_expr(function.args, schema, planner_context)
                .await?;
            return Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)));
        };

        // If function is a window function (it has an OVER clause),
        // it shouldn't have ordering requirement as function argument
        // required ordering should be defined in OVER clause.
        let is_function_window = function.over.is_some();
        if !function.order_by.is_empty() && is_function_window {
            return Err(DataFusionError::Plan(
                "Aggregate ORDER BY is not implemented for window functions".to_string(),
            ));
        }

        // then, window function
        if let Some(WindowType::WindowSpec(window)) = function.over.take() {
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
            let order_by = self
                .order_by_to_sort_expr(&window.order_by, schema, planner_context)
                .await?;
            let window_frame = window
                .window_frame
                .as_ref()
                .map(|window_frame| {
                    let window_frame = window_frame.clone().try_into()?;
                    regularize(window_frame, order_by.len())
                })
                .transpose()?;
            let window_frame = if let Some(window_frame) = window_frame {
                window_frame
            } else {
                WindowFrame::new(!order_by.is_empty())
            };
            if let Ok(fun) = self.find_window_func(&name).await {
                let expr = match fun {
                    WindowFunction::AggregateFunction(aggregate_fun) => {
                        let (aggregate_fun, args) = self
                            .aggregate_fn_to_expr(
                                aggregate_fun,
                                function.args,
                                schema,
                                planner_context,
                            )
                            .await?;

                        Expr::WindowFunction(expr::WindowFunction::new(
                            WindowFunction::AggregateFunction(aggregate_fun),
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                        ))
                    }
                    _ => Expr::WindowFunction(expr::WindowFunction::new(
                        fun,
                        self.function_args_to_expr(function.args, schema, planner_context)
                            .await?,
                        partition_by,
                        order_by,
                        window_frame,
                    )),
                };
                return Ok(expr);
            }
        } else {
            // next, aggregate built-ins
            if let Ok(fun) = AggregateFunction::from_str(&name) {
                let distinct = function.distinct;
                let order_by = self
                    .order_by_to_sort_expr(&function.order_by, schema, planner_context)
                    .await?;
                let order_by = (!order_by.is_empty()).then_some(order_by);
                let (fun, args) = self
                    .aggregate_fn_to_expr(fun, function.args, schema, planner_context)
                    .await?;
                return Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                    fun, args, distinct, None, order_by,
                )));
            };

            // finally, user-defined functions (UDF) and UDAF
            if let Some(fm) = self.schema_provider.get_function_meta(&name).await {
                let args = self
                    .function_args_to_expr(function.args, schema, planner_context)
                    .await?;
                return Ok(Expr::ScalarUDF(ScalarUDF::new(fm, args)));
            }

            // User defined aggregate functions
            if let Some(fm) = self.schema_provider.get_aggregate_meta(&name).await {
                let args = self
                    .function_args_to_expr(function.args, schema, planner_context)
                    .await?;
                return Ok(Expr::AggregateUDF(expr::AggregateUDF::new(
                    fm, args, None, None,
                )));
            }

            // Special case arrow_cast (as its type is dependent on its argument value)
            if name == ARROW_CAST_NAME {
                let args = self
                    .function_args_to_expr(function.args, schema, planner_context)
                    .await?;
                return super::arrow_cast::create_arrow_cast(args, schema);
            }
        }

        // Could not find the relevant function, so return an error
        Err(DataFusionError::Plan(format!("Invalid function '{name}'.")))
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

    pub(super) async fn find_window_func(&mut self, name: &str) -> Result<WindowFunction> {
        let window_fn = if let Some(window_fn) = window_function::find_df_window_func(name) {
            Some(window_fn)
        } else {
            self.schema_provider
                .get_aggregate_meta(name)
                .await
                .map(WindowFunction::AggregateUDF)
        };
        window_fn.ok_or_else(|| {
            DataFusionError::Plan(format!("There is no window function named {name}"))
        })
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
            } => Ok(Expr::Wildcard),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.sql_expr_to_logical_expr(arg, schema, planner_context)
                    .await
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expr::Wildcard),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported qualified wildcard argument: {sql:?}"
            ))),
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

    pub(super) async fn aggregate_fn_to_expr(
        &mut self,
        fun: AggregateFunction,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(AggregateFunction, Vec<Expr>)> {
        let args = match fun {
            // Special case rewrite COUNT(*) to COUNT(constant)
            AggregateFunction::Count => {
                let mut exprs = Vec::with_capacity(args.len());
                for a in args {
                    let e = match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                            Expr::Literal(COUNT_STAR_EXPANSION.clone())
                        }
                        _ => {
                            self.sql_fn_arg_to_logical_expr(a, schema, planner_context)
                                .await?
                        }
                    };
                    exprs.push(e);
                }
                exprs
            }
            _ => {
                self.function_args_to_expr(args, schema, planner_context)
                    .await?
            }
        };

        Ok((fun, args))
    }
}
