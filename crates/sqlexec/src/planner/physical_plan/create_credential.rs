use super::*;

use protogen::metastore::types::{options::CredentialsOptions, service, service::Mutation};
use protogen::sqlexec::physical_plan::ExecutionPlanExtensionType;

use crate::planner::errors::internal;
use catalog::session_catalog::CatalogMutator;
use protogen::export::prost::Message;

#[derive(Clone, Debug)]
pub struct CreateCredentialExec {
    pub name: String,
    pub catalog_version: u64,
    pub options: CredentialsOptions,
    pub comment: String,
}

impl PhysicalExtensionNode for CreateCredentialExec {
    type ProtoRepr = protogen::sqlexec::physical_plan::CreateCredentialExec;

    fn try_encode(
        &self,
        buf: &mut Vec<u8>,
        _codec: &dyn datafusion_proto::physical_plan::PhysicalExtensionCodec,
    ) -> crate::errors::Result<()> {
        let proto = protogen::sqlexec::physical_plan::CreateCredentialExec {
            name: self.name.clone(),
            catalog_version: self.catalog_version,
            options: Some(self.options.clone().into()),
            comment: self.comment.clone(),
        };
        let ty = ExecutionPlanExtensionType::CreateCredentialExec(proto);
        let extension =
            protogen::sqlexec::physical_plan::ExecutionPlanExtension { inner: Some(ty) };
        extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;
        Ok(())
    }

    fn try_decode(
        proto: Self::ProtoRepr,
        _registry: &dyn FunctionRegistry,
        _runtime: &RuntimeEnv,
        _extension_codec: &dyn PhysicalExtensionCodec,
    ) -> crate::errors::Result<Self, protogen::ProtoConvError> {
        let options = proto
            .options
            .ok_or(protogen::ProtoConvError::RequiredField(
                "options".to_string(),
            ))?;

        Ok(Self {
            name: proto.name,
            catalog_version: proto.catalog_version,
            options: options.try_into()?,
            comment: proto.comment,
        })
    }
}

impl DisplayAs for CreateCredentialExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateCredentialExec")
    }
}

impl ExecutionPlan for CreateCredentialExec {
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
    plan: CreateCredentialExec,
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
        .map_err(|e| DataFusionError::Execution(format!("failed to create credential: {e}")))?;

    Ok(new_operation_batch("create_credential"))
}
