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

use std::sync::Arc;

use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{Expr, Subquery};
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast::{Expr as SQLExpr, Query};

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    pub(super) async fn parse_exists_subquery(
        &mut self,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let old_outer_query_schema =
            planner_context.set_outer_query_schema(Some(input_schema.clone()));
        let sub_plan = self
            .query_to_plan_with_context(subquery, planner_context)
            .await?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        planner_context.set_outer_query_schema(old_outer_query_schema);
        Ok(Expr::Exists(Exists {
            subquery: Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
            },
            negated,
        }))
    }

    pub(super) async fn parse_in_subquery(
        &mut self,
        expr: SQLExpr,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let old_outer_query_schema =
            planner_context.set_outer_query_schema(Some(input_schema.clone()));
        let sub_plan = self
            .query_to_plan_with_context(subquery, planner_context)
            .await?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        planner_context.set_outer_query_schema(old_outer_query_schema);
        let expr = Box::new(
            self.sql_to_expr(expr, input_schema, planner_context)
                .await?,
        );
        Ok(Expr::InSubquery(InSubquery::new(
            expr,
            Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
            },
            negated,
        )))
    }

    pub(super) async fn parse_scalar_subquery(
        &mut self,
        subquery: Query,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let old_outer_query_schema =
            planner_context.set_outer_query_schema(Some(input_schema.clone()));
        let sub_plan = self
            .query_to_plan_with_context(subquery, planner_context)
            .await?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        planner_context.set_outer_query_schema(old_outer_query_schema);
        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(sub_plan),
            outer_ref_columns,
        }))
    }
}
