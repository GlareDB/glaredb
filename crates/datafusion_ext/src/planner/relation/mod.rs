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
use std::path::Path;

use crate::functions::FuncParamValue;
use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

use async_recursion::async_recursion;
use datafusion::common::{DataFusionError, OwnedTableReference, Result};

use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};

use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast;

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
                mut name,
                alias,
                args,
                ..
            } => {
                if name.0.len() == 1 && name.0[0].quote_style == Some('\'') {
                    // SELECT * FROM './my/file.csv'
                    //
                    // Infer the table function to use based on a file path.

                    let path = name.0.pop().unwrap().value;

                    let func_ref = infer_func_for_file(&path)?;
                    let args = vec![FuncParamValue::Scalar(ScalarValue::Utf8(Some(
                        path.clone(),
                    )))];
                    let func = self
                        .schema_provider
                        .get_table_func(func_ref, args, HashMap::new())
                        .await?;

                    let table_ref = OwnedTableReference::Bare { table: path.into() };
                    let plan_builder = LogicalPlanBuilder::scan(table_ref, func, None)?;
                    let plan = plan_builder.build()?;

                    (plan, alias)
                } else {
                    // SELECT * FROM my_table
                    // SELECT * FROM func(...)

                    // normalize name and alias
                    let table_ref = self.object_name_to_table_reference(name)?;
                    let mut unnamed_args = Vec::new();
                    let mut named_args = HashMap::new();

                    match args {
                        Some(args) => {
                            // Table factor has arguments, look up table returning
                            // function.
                            for arg in args {
                                let (name, val) = self.get_constant_function_arg(arg)?;
                                if let Some(name) = name {
                                    named_args.insert(name, val);
                                } else {
                                    unnamed_args.push(val);
                                }
                            }
                            let provider = self
                                .schema_provider
                                .get_table_func(table_ref.clone(), unnamed_args, named_args)
                                .await?;

                            let plan_builder = LogicalPlanBuilder::scan(table_ref, provider, None)?;

                            let plan = plan_builder.build()?;

                            (plan, alias)
                        }
                        None => {
                            let table_name = table_ref.to_string();

                            let cte = planner_context.get_cte(&table_name);
                            let plan = if let Some(cte_plan) = cte {
                                cte_plan.clone()
                            } else {
                                let provider = self
                                    .schema_provider
                                    .get_table_provider(table_ref.clone())
                                    .await?;
                                let plan_builder =
                                    LogicalPlanBuilder::scan(table_ref, provider, None)?;
                                plan_builder.build()?
                            };
                            (plan, alias)
                        }
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
            ast::Expr::Identifier(ident) => {
                Ok(FuncParamValue::Ident(self.normalizer.normalize(ident)))
            }
            ast::Expr::Array(arr) => {
                let arr = arr
                    .elem
                    .iter()
                    .map(|e| self.get_param_val(e.clone()))
                    .collect::<Result<Vec<FuncParamValue>>>();
                Ok(FuncParamValue::Array(arr?))
            }
            ast::Expr::UnaryOp { op, expr } => match op {
                ast::UnaryOperator::Minus => {
                    match *expr {
                        // optimization: if it's a number literal, we apply the negative operator
                        // here directly to calculate the new literal.
                        ast::Expr::Value(ast::Value::Number(n, _)) => match n.parse::<i64>() {
                            Ok(n) => Ok(FuncParamValue::Scalar(ScalarValue::Int64(Some(-n)))),
                            Err(_) => {
                                let n = n.parse::<f64>().map_err(|_e| {
                                    DataFusionError::Internal(format!(
                                        "negative operator can be only applied to integer and float operands, got: {n}"))
                                })?;
                                Ok(FuncParamValue::Scalar(ScalarValue::Float64(Some(-n))))
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
                Ok(datafusion::logical_expr::expr::Expr::Literal(lit)) => {
                    Ok(FuncParamValue::Scalar(lit))
                }
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

/// Returns a reference to table func by inferring which function to use from a
/// given path.
fn infer_func_for_file(path: &str) -> Result<OwnedTableReference> {
    let ext = Path::new(path)
        .extension()
        .ok_or_else(|| DataFusionError::Plan(format!("missing file extension: {path}")))?
        .to_str()
        .ok_or_else(|| DataFusionError::Plan(format!("strange file extension: {path}")))?
        .to_lowercase();

    // TODO: We can be a bit more sophisticated here and handle compression
    // schemes as well.
    Ok(match ext.as_str() {
        "parquet" => OwnedTableReference::Bare {
            table: "parquet_scan".into(),
        },
        "csv" => OwnedTableReference::Bare {
            table: "csv_scan".into(),
        },
        "json" | "jsonl" => OwnedTableReference::Bare {
            table: "ndjson_scan".into(),
        },
        ext => {
            return Err(DataFusionError::Plan(format!(
                "unable to infer how to handle file extension: {ext}"
            )))
        }
    })
}
