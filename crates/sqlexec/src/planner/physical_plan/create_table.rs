use datafusion_proto::protobuf::PhysicalPlanNode;
use protogen::{
    metastore::types::{service, service::Mutation},
    ProtoConvError,
};

use crate::metastore::catalog::CatalogMutator;

use super::*;

#[derive(Debug, Clone)]
pub struct CreateTableExec {
    pub catalog_version: u64,
    pub schema: String,
    pub name: String,
    pub if_not_exists: bool,
    pub arrow_schema: SchemaRef,
    pub source: Option<Arc<dyn ExecutionPlan>>,
}

impl PhysicalExtensionNode for CreateTableExec {
    type ProtoRepr = protogen::sqlexec::physical_plan::CreateTableExec;

    fn try_encode(
        &self,
        buf: &mut Vec<u8>,
        _codec: &dyn PhysicalExtensionCodec,
    ) -> crate::errors::Result<()> {
        todo!()
    }

    fn try_decode(
        proto: Self::ProtoRepr,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> crate::errors::Result<Self, protogen::ProtoConvError> {
        let source = proto
            .source
            .map(|src| src.try_into_physical_plan(registry, runtime, extension_codec))
            .transpose()
            .map_err(ProtoConvError::DataFusionError)?;
        todo!()
    }
}

impl DisplayAs for CreateTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateTableExec")
    }
}
impl ExecutionPlan for CreateTableExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        match &self.source {
            Some(source) => vec![source.clone()],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let catalog_mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .unwrap();
        let stream = stream::once(create_table(self.clone(), catalog_mutator)).boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

async fn create_table(
    plan: CreateTableExec,
    mutator: Arc<CatalogMutator>,
) -> DataFusionResult<RecordBatch> {
    mutator
        .mutate(
            plan.catalog_version,
            [Mutation::CreateTable(service::CreateTable {
                schema: plan.schema,
                name: plan.name,
                options: plan.arrow_schema.into(),
                if_not_exists: plan.if_not_exists,
            })],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to create credentials: {e}")))?;

    Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
}
