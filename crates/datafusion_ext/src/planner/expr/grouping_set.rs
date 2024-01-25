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

use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion::logical_expr::{Expr, GroupingSet};
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast::Expr as SQLExpr;

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    pub(super) async fn sql_grouping_sets_to_expr(
        &mut self,
        exprs: Vec<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut args = Vec::with_capacity(exprs.len());
        for v in exprs {
            let mut ve = Vec::with_capacity(v.len());
            for e in v {
                let e = self
                    .sql_expr_to_logical_expr(e, schema, planner_context)
                    .await?;
                ve.push(e);
            }
            args.push(ve);
        }
        Ok(Expr::GroupingSet(GroupingSet::GroupingSets(args)))
    }

    pub(super) async fn sql_rollup_to_expr(
        &mut self,
        exprs: Vec<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut args = Vec::with_capacity(exprs.len());
        for v in exprs {
            if v.len() != 1 {
                return Err(DataFusionError::Internal(
                    "Tuple expressions are not supported for Rollup expressions".to_string(),
                ));
            }
            let v = self
                .sql_expr_to_logical_expr(v[0].clone(), schema, planner_context)
                .await?;
            args.push(v);
        }
        Ok(Expr::GroupingSet(GroupingSet::Rollup(args)))
    }

    pub(super) async fn sql_cube_to_expr(
        &mut self,
        exprs: Vec<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut args = Vec::with_capacity(exprs.len());
        for v in exprs {
            if v.len() != 1 {
                return Err(DataFusionError::Internal(
                    "Tuple expressions not are supported for Cube expressions".to_string(),
                ));
            }
            let v = self
                .sql_expr_to_logical_expr(v[0].clone(), schema, planner_context)
                .await?;
            args.push(v);
        }
        Ok(Expr::GroupingSet(GroupingSet::Cube(args)))
    }
}
