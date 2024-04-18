use catalog::mutator::CatalogMutator;
use protogen::metastore::types::options::CredentialsOptions;
use protogen::metastore::types::service;
use protogen::metastore::types::service::Mutation;

use super::{
    new_operation_batch,
    stream,
    Arc,
    DataFusionError,
    DataFusionResult,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    PhysicalSortExpr,
    RecordBatch,
    RecordBatchStreamAdapter,
    SchemaRef,
    Statistics,
    StreamExt,
    GENERIC_OPERATION_PHYSICAL_SCHEMA,
};

#[derive(Clone, Debug)]
pub struct CreateCredentialsExec {
    pub name: String,
    pub catalog_version: u64,
    pub options: CredentialsOptions,
    pub comment: String,
    pub or_replace: bool,
}

impl DisplayAs for CreateCredentialsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateCredentialsExec")
    }
}

impl ExecutionPlan for CreateCredentialsExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        GENERIC_OPERATION_PHYSICAL_SCHEMA.clone()
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Cannot change children for CreateCredentialsExec".to_string(),
            ))
        }
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

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

async fn create_credentials(
    plan: CreateCredentialsExec,
    mutator: Arc<CatalogMutator>,
) -> DataFusionResult<RecordBatch> {
    mutator
        .mutate_and_commit(
            plan.catalog_version,
            [Mutation::CreateCredentials(service::CreateCredentials {
                name: plan.name,
                options: plan.options,
                comment: plan.comment,
                or_replace: plan.or_replace,
            })],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to create credentials: {e}")))?;

    Ok(new_operation_batch("create_credentials"))
}
