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

use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::planner::PlannerContext;
use parser::sqlparser::ast::Values as SQLValues;

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    pub(super) async fn sql_values_to_plan(
        &mut self,
        values: SQLValues,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let SQLValues {
            explicit_row: _,
            rows,
        } = values;

        // values should not be based on any other schema
        let schema = DFSchema::empty();
        let values = {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let mut ve = Vec::with_capacity(row.len());
                for v in row {
                    let v = self.sql_to_expr(v, &schema, planner_context).await?;
                    ve.push(v);
                }
                values.push(ve);
            }
            values
        };
        LogicalPlanBuilder::values(values)?.build()
    }
}
