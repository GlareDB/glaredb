use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::prelude::Expr;
use datafusion::{
    arrow::datatypes::Schema,
    datasource::TableProvider,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use std::{any::Any, sync::Arc};

use crate::execution::physical_planner::SessionPhysicalPlanner;

pub struct ViewTable {
    schema: Arc<Schema>,
    plan: LogicalPlan,
    planner: SessionPhysicalPlanner,
}

impl ViewTable {
    /// Create a new table provider backed by some logical plan.
    ///
    /// During scan, the plan will be converted into a physical plan. The plan
    /// provided should already have been optimized.
    pub fn new(plan: LogicalPlan, planner: SessionPhysicalPlanner) -> ViewTable {
        let schema: Schema = plan.schema().as_ref().into();
        ViewTable {
            schema: Arc::new(schema),
            plan,
            planner,
        }
    }
}

#[async_trait]
impl TableProvider for ViewTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Exact)
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Adapted from datafusion's view table source.
        let plan = if let Some(projection) = projection {
            // avoiding adding a redundant projection (e.g. SELECT * FROM view)
            let current_projection = (0..self.plan.schema().fields().len()).collect::<Vec<usize>>();
            if projection == &current_projection {
                self.plan.clone()
            } else {
                let fields: Vec<Expr> = projection
                    .iter()
                    .map(|i| Expr::Column(self.plan.schema().field(*i).qualified_column()))
                    .collect();
                LogicalPlanBuilder::from(self.plan.clone())
                    .project(fields)?
                    .build()?
            }
        } else {
            self.plan.clone()
        };
        let mut plan = LogicalPlanBuilder::from(plan);
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));

        if let Some(filter) = filter {
            plan = plan.filter(filter)?;
        }

        if let Some(limit) = limit {
            plan = plan.limit(0, Some(limit))?;
        }

        let plan = plan.build()?;

        self.planner.create_physical_plan(&plan, state).await
    }
}
