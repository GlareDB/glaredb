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
use async_recursion::async_recursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, TableSource};
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast::{
    self, FunctionArg, FunctionArgExpr, ObjectName, TableFactor, Value,
};
use datasources::object_store::local::{LocalAccessor, LocalTableAccess};
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};
use std::sync::Arc;

mod join;

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    /// Create a `LogicalPlan` that scans the named relation
    #[async_recursion]
    async fn create_relation(
        &mut self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                match args {
                    Some(args) => {
                        let table_ref = self.object_name_to_table_reference(name)?;
                        let table_name = table_ref.to_string();

                        let source = table_returning_function(&table_name, args).await?;
                        let plan = LogicalPlanBuilder::scan(table_name, source, None)?.build()?;
                        (plan, alias)
                    }
                    None => {
                        // normalize name and alias
                        let table_ref = self.object_name_to_table_reference(name)?;
                        let table_name = table_ref.to_string();
                        let cte = planner_context.get_cte(&table_name);
                        (
                            match (
                                cte,
                                self.schema_provider
                                    .get_table_provider(table_ref.clone())
                                    .await,
                            ) {
                                (Some(cte_plan), _) => Ok(cte_plan.clone()),
                                (_, Ok(provider)) => {
                                    LogicalPlanBuilder::scan(table_ref, provider, None)?.build()
                                }
                                (None, Err(e)) => Err(e),
                            }?,
                            alias,
                        )
                    }
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self
                    .query_to_plan_with_schema(*subquery, planner_context)
                    .await?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
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
}

// jank
async fn table_returning_function(
    mut name: &str,
    mut args: Vec<ast::FunctionArg>,
) -> Result<Arc<dyn TableSource>> {
    match name {
        "read_csv" => {
            let file = args.pop().unwrap();
            let file = arg_to_string(file).unwrap();
            let access = LocalTableAccess {
                location: file,
                file_type: None,
            };
            let prov = LocalAccessor::new(access)
                .await
                .unwrap()
                .into_table_provider(true)
                .await
                .unwrap();
            Ok(Arc::new(DefaultTableSource::new(prov)))
        }
        "read_postgres" => {
            let name = arg_to_string(args.pop().unwrap()).unwrap();
            let schema = arg_to_string(args.pop().unwrap()).unwrap();
            let conn_str = arg_to_string(args.pop().unwrap()).unwrap();
            let access = PostgresTableAccess { schema, name };
            let prov = PostgresAccessor::connect(&conn_str, None)
                .await
                .unwrap()
                .into_table_provider(access, true)
                .await
                .unwrap();
            Ok(Arc::new(DefaultTableSource::new(Arc::new(prov))))
        }
        _ => unimplemented!(),
    }
}

fn arg_to_string(arg: FunctionArg) -> Result<String> {
    match arg {
        FunctionArg::Unnamed(expr) => match expr {
            FunctionArgExpr::Expr(ast::Expr::Value(v)) => match v {
                Value::SingleQuotedString(s) => Ok(s),
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}
