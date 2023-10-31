use super::*;
use std::{any::Any, sync::Arc};

use arrow_util::pretty::fmt_dtype;
use datafusion::{
    arrow::{
        array::{BooleanBuilder, StringBuilder},
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    error::Result,
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext::vars::SessionVars;
use futures::stream;
use protogen::sqlexec::physical_plan::ExecutionPlanExtensionType;

use crate::{
    metastore::catalog::{CatalogMutator, SessionCatalog, TempCatalog},
    planner::{
        errors::PlanError,
        logical_plan::{OwnedFullObjectReference, DESCRIBE_TABLE_SCHEMA},
    },
    resolve::EntryResolver,
};
use crate::{planner::errors::internal, resolve::ResolvedEntry};
use protogen::export::prost::Message;

#[derive(Debug, Clone)]
pub struct DescribeTableExec {
    pub tbl_reference: OwnedFullObjectReference,
}

impl DisplayAs for DescribeTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DescribeTableExec")
    }
}

impl PhysicalExtensionNode for DescribeTableExec {
    type ProtoRepr = protogen::sqlexec::physical_plan::DescribeTableExec;

    fn try_encode(
        &self,
        buf: &mut Vec<u8>,
        _codec: &dyn datafusion_proto::physical_plan::PhysicalExtensionCodec,
    ) -> crate::errors::Result<()> {
        let proto = protogen::sqlexec::physical_plan::DescribeTableExec {
            tbl_reference: Some(self.tbl_reference.clone().into()),
        };
        let ty = ExecutionPlanExtensionType::DescribeTable(proto);
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
        Ok(Self {
            tbl_reference: proto.tbl_reference.unwrap().try_into().unwrap(),
        })
    }
}

impl ExecutionPlan for DescribeTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        DESCRIBE_TABLE_SCHEMA.clone()
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
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot change children for DescribeTableExec".to_string(),
        ))
    }

    fn execute(&self, _: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let catalog = context
            .session_config()
            .get_extension::<SessionCatalog>()
            .expect("context should have catalog");

        let temp_catalog = context.session_config().get_extension::<TempCatalog>();

        let vars = context
            .session_config()
            .options()
            .extensions
            .get::<SessionVars>()
            .expect("context should have SessionVars");

        let stream = stream::once(describe_table(
            self.tbl_reference.clone(),
            mutator,
            catalog,
            temp_catalog,
            Arc::new(vars.clone()),
        ));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

async fn describe_table(
    tbl_ref: OwnedFullObjectReference,
    mutator: Arc<CatalogMutator>,
    catalog: Arc<SessionCatalog>,
    temp_catalog: Option<Arc<TempCatalog>>,
    vars: Arc<SessionVars>,
) -> Result<RecordBatch> {
    let metastore_client = mutator.get_metastore_client();
    let mut catalog = catalog.as_ref().clone();
    let schema_search_path = vars.implicit_search_path();
    let temp_catalog = temp_catalog.unwrap_or_default();

    let _ = catalog
        .maybe_refresh_state(metastore_client, false)
        .await
        .unwrap();
    let resolver = EntryResolver {
        catalog: &catalog,
        temp_objects: temp_catalog,
        schema_search_path,
    };

    let ent = resolver
        .resolve_entry_from_reference(tbl_ref.into())
        .and_then(ResolvedEntry::try_into_table_entry)
        .expect("entry should be checked during planning");

    let internal_cols = match ent.get_internal_columns() {
        Some(cols) => cols,
        None => {
            return Err(PlanError::UnsupportedFeature(
                "'DESCRIBE' not yet supported for external tables",
            )
            .into())
        }
    };

    let mut column_names = StringBuilder::new();
    let mut data_types = StringBuilder::new();
    let mut is_nullables = BooleanBuilder::new();

    for col in internal_cols {
        let name = col.name.clone();
        let data_type = col.arrow_type.clone();

        column_names.append_value(name);

        data_types.append_value(fmt_dtype(&data_type));

        is_nullables.append_value(col.nullable);
    }

    let output_schema = DESCRIBE_TABLE_SCHEMA.clone();

    let record_batch = RecordBatch::try_new(
        output_schema,
        vec![
            Arc::new(column_names.finish()),
            Arc::new(data_types.finish()),
            Arc::new(is_nullables.finish()),
        ],
    )?;

    Ok(record_batch)
}
