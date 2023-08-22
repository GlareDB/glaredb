use datafusion_proto::protobuf::PhysicalPlanNode;
use protogen::{
    metastore::types::{service, service::Mutation},
    ProtoConvError,
};
use sqlbuiltins::builtins::DEFAULT_CATALOG;

use crate::{
    errors::ExecError,
    metastore::catalog::{CatalogMutator, SessionCatalog},
    planner::errors::internal,
};

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
        codec: &dyn PhysicalExtensionCodec,
    ) -> crate::errors::Result<()> {
        let proto = protogen::sqlexec::physical_plan::CreateTableExec {
            catalog_version: self.catalog_version,
            schema: self.schema.clone(),
            name: self.name.clone(),
            if_not_exists: self.if_not_exists,
            arrow_schema: Some(self.arrow_schema.clone().try_into().unwrap()),
            source: self
                .source
                .clone()
                .map(|src| PhysicalPlanNode::try_from_physical_plan(src, codec))
                .transpose()?,
        };

        let ty =
            protogen::sqlexec::physical_plan::ExecutionPlanExtensionType::CreateTableExec(proto);
        let extension =
            protogen::sqlexec::physical_plan::ExecutionPlanExtension { inner: Some(ty) };
        extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;
        Ok(())
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

        let arrow_schema = &proto.arrow_schema.ok_or(ProtoConvError::RequiredField(
            "schema name is required".to_string(),
        ))?;
        let arrow_schema: Schema = arrow_schema.try_into()?;

        Ok(Self {
            catalog_version: proto.catalog_version,
            schema: proto.schema,
            name: proto.name,
            if_not_exists: proto.if_not_exists,
            arrow_schema: Arc::new(arrow_schema),
            source,
        })
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        _partition: usize,
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
    let state = mutator
        .mutate(
            plan.catalog_version,
            [Mutation::CreateTable(service::CreateTable {
                schema: plan.schema.clone(),
                name: plan.name.clone(),
                options: plan.arrow_schema.into(),
                if_not_exists: plan.if_not_exists,
            })],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to create table: {e}")))?;
    let new_catalog = SessionCatalog::new(state);
    let ent = new_catalog
        .resolve_native_table(DEFAULT_CATALOG, &plan.schema, &plan.name)
        .ok_or_else(|| ExecError::Internal("Missing table after catalog insert".to_string()))
        .unwrap();
    todo!("need access to native tables");
}
