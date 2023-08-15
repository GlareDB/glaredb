use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, Statistics};
use protogen::metastore::types::options::TunnelOptions;

use crate::planner::EMPTY_SCHEMA;

#[derive(Debug)]
pub struct CreateTunnelExec {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TunnelOptions,
}

impl DisplayAs for CreateTunnelExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for CreateTunnelExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        EMPTY_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        Statistics::default()
    }
}
