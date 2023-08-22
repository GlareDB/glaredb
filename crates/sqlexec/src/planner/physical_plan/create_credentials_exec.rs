use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan};
use datafusion::physical_plan::{Partitioning, Statistics};
use futures::stream;
use futures::StreamExt;
use protogen::metastore::types::{options::CredentialsOptions, service, service::Mutation};

use crate::metastore::catalog::CatalogMutator;

#[derive(Clone, Debug)]
pub struct CreateCredentialsExec {
    pub name: String,
    pub catalog_version: u64,
    pub options: CredentialsOptions,
    pub comment: String,
}

impl DisplayAs for CreateCredentialsExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CreateCredentialsExec")
    }
}

impl ExecutionPlan for CreateCredentialsExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot change children for CreateCredentialsExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<datafusion::physical_plan::SendableRecordBatchStream> {
        let catalog_mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .unwrap();
        let stream = stream::once(create_credentials(self.clone(), catalog_mutator)).boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

async fn create_credentials(
    plan: CreateCredentialsExec,
    mutator: Arc<CatalogMutator>,
) -> DataFusionResult<RecordBatch> {
    mutator
        .mutate(
            plan.catalog_version,
            [Mutation::CreateCredentials(service::CreateCredentials {
                name: plan.name,
                options: plan.options,
                comment: plan.comment,
            })],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to create schema: {e}")))?;

    Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
}
