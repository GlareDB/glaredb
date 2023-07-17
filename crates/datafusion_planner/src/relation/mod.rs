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

use std::collections::HashMap;
use std::sync::Arc;

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};
use async_recursion::async_recursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast;
use sqlbuiltins::functions::FuncParamValue;

mod join;

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    /// Create a `LogicalPlan` that scans the named relation
    #[async_recursion]
    async fn create_relation(
        &mut self,
        relation: ast::TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            ast::TableFactor::Table {
                name, alias, args, ..
            } => {
                // normalize name and alias
                let table_ref = self.object_name_to_table_reference(name)?;

                match args {
                    Some(args) => {
                        // Table factor has arguments, look up table returning
                        // function.

                        let mut unnamed_args = Vec::new();
                        let mut named_args = HashMap::new();
                        for arg in args {
                            let (name, val) = self.get_constant_function_arg(arg)?;
                            if let Some(name) = name {
                                named_args.insert(name, val);
                            } else {
                                unnamed_args.push(val);
                            }
                        }

                        let func = self
                            .schema_provider
                            .get_table_func(table_ref.clone())
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "Missing table function: '{table_ref}'"
                                ))
                            })?;

                        let table_fn_provider = self.schema_provider.table_fn_ctx_provider();
                        let provider = func
                            .create_provider(&table_fn_provider, unnamed_args, named_args)
                            .await
                            .map_err(|e| DataFusionError::Plan(e.to_string()))?;

                        let source = Arc::new(DefaultTableSource::new(provider));

                        let plan_builder = LogicalPlanBuilder::scan(table_ref, source, None)?;
                        let plan = plan_builder.build()?;

                        (plan, alias)
                    }
                    None => {
                        // No arguments provided. Just a basic table.

                        let table_name = table_ref.to_string();

                        let cte = planner_context.get_cte(&table_name);
                        let plan = if let Some(cte_plan) = cte {
                            cte_plan.clone()
                        } else {
                            let provider = self
                                .schema_provider
                                .get_table_provider(table_ref.clone())
                                .await?;
                            let plan_builder = LogicalPlanBuilder::scan(table_ref, provider, None)?;
                            plan_builder.build()?
                        };
                        (plan, alias)
                    }
                }
            }
            ast::TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self
                    .query_to_plan_with_context(*subquery, planner_context)
                    .await?;
                (logical_plan, alias)
            }
            ast::TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => (
                self.plan_table_with_joins(*table_with_joins, planner_context)
                    .await?,
                alias,
            ),
            // @todo Support TableFactory::TableFunction?
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported ast node {relation:?} in create_relation"
                )));
            }
        };
        if let Some(alias) = alias {
            self.apply_table_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }

    /// Get a constant expression literal from a function argument.
    ///
    /// Returns an optional name for the argument.
    fn get_constant_function_arg(
        &mut self,
        arg: ast::FunctionArg,
    ) -> Result<(Option<String>, FuncParamValue)> {
        match arg {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                Ok((None, self.get_param_val(expr)?))
            }
            ast::FunctionArg::Named {
                name,
                arg: ast::FunctionArgExpr::Expr(expr),
            } => {
                let name = self.normalizer.normalize(name);
                Ok((Some(name), self.get_param_val(expr)?))
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Non-constant function argument: {other:?}",
            ))),
        }
    }

    /// Get the parameter value from expr.
    fn get_param_val(&self, expr: ast::Expr) -> Result<FuncParamValue> {
        match expr {
            ast::Expr::Identifier(ident) => Ok(self.normalizer.normalize(ident).into()),
            ast::Expr::UnaryOp { op, expr } => match op {
                ast::UnaryOperator::Minus => {
                    match *expr {
                        // optimization: if it's a number literal, we apply the negative operator
                        // here directly to calculate the new literal.
                        ast::Expr::Value(ast::Value::Number(n, _)) => match n.parse::<i64>() {
                            Ok(n) => Ok(ScalarValue::Int64(Some(-n)).into()),
                            Err(_) => {
                                let n = n.parse::<f64>().map_err(|_e| {
                                    DataFusionError::Internal(format!(
                                        "negative operator can be only applied to integer and float operands, got: {n}"))
                                })?;
                                Ok(ScalarValue::Float64(Some(-n)).into())
                            }
                        },
                        other => Err(DataFusionError::NotImplemented(format!(
                            "Non-constant function argument: {other:?}",
                        ))),
                    }
                }
                other => Err(DataFusionError::NotImplemented(format!(
                    "Non-constant function argument: {other:?}",
                ))),
            },

            ast::Expr::Value(v) => match self.parse_value(v, &[]) {
                Ok(Expr::Literal(lit)) => Ok(lit.into()),
                Ok(v) => Err(DataFusionError::NotImplemented(format!(
                    "Non-constant function argument: {v:?}"
                ))),
                Err(e) => Err(e),
            },

            other => Err(DataFusionError::NotImplemented(format!(
                "Non-constant function argument: {other:?}",
            ))),
        }
    }
}
