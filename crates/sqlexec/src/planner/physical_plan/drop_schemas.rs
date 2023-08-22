use crate::metastore::catalog::CatalogMutator;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use futures::stream;
use protogen::metastore::types::service::{self, Mutation};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DropSchemasExec {
    pub catalog_version: u64,
    pub names: Vec<String>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl ExecutionPlan for DropSchemasExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::empty())
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot change children for DropSchemasExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "DropSchemasExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(drop_schemas(mutator, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for DropSchemasExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropSchemasExec")
    }
}

async fn drop_schemas(
    mutator: Arc<CatalogMutator>,
    plan: DropSchemasExec,
) -> DataFusionResult<RecordBatch> {
    let drops: Vec<_> = plan
        .names
        .into_iter()
        .map(|name| {
            Mutation::DropSchema(service::DropSchema {
                name,
                if_exists: plan.if_exists,
                cascade: plan.cascade,
            })
        })
        .collect();

    mutator
        .mutate(plan.catalog_version, drops)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to drop schemas: {e}")))?;

    Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
}
