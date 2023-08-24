use std::sync::Arc;

use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use datasources::native::access::NativeTableStorage;
use futures::stream;
use protogen::metastore::types::{service, service::Mutation};
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use tracing::info;

use crate::{
    errors::ExecError,
    metastore::catalog::{CatalogMutator, SessionCatalog},
    planner::{logical_plan::OwnedFullObjectReference, physical_plan::insert::InsertExec},
};

use super::insert::INSERT_COUNT_SCHEMA;

#[derive(Debug, Clone)]
pub struct CreateTableExec {
    pub catalog_version: u64,
    pub reference: OwnedFullObjectReference,
    pub if_not_exists: bool,
    pub arrow_schema: SchemaRef,
    pub source: Option<Arc<dyn ExecutionPlan>>,
}

impl ExecutionPlan for CreateTableExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        if self.source.is_some() {
            Arc::new(Schema::empty())
        } else {
            INSERT_COUNT_SCHEMA.clone()
        }
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
        Ok(Arc::new(CreateTableExec {
            catalog_version: self.catalog_version,
            reference: self.reference.clone(),
            if_not_exists: self.if_not_exists,
            arrow_schema: self.arrow_schema.clone(),
            source: children.get(0).cloned(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "CreateTableExec only supports 1 partition".to_string(),
            ));
        }

        let catalog_mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .unwrap();
        let storage = context
            .session_config()
            .get_extension::<NativeTableStorage>()
            .unwrap();

        let this = self.clone();
        let stream = stream::once(this.create_table(catalog_mutator, storage, context));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for CreateTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateTableExec")
    }
}

impl CreateTableExec {
    async fn create_table(
        self,
        mutator: Arc<CatalogMutator>,
        storage: Arc<NativeTableStorage>,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<RecordBatch> {
        let schema = self.schema();

        let state = mutator
            .mutate(
                self.catalog_version,
                [Mutation::CreateTable(service::CreateTable {
                    schema: self.reference.schema.clone().into_owned(),
                    name: self.reference.name.clone().into_owned(),
                    options: self.arrow_schema.into(),
                    if_not_exists: self.if_not_exists,
                })],
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("failed to create table in catalog: {e}"))
            })?;

        // Note that we're not changing out the catalog stored on the context,
        // we're just using it here to get the new table entry easily.
        let new_catalog = SessionCatalog::new(state);
        let ent = new_catalog
            .resolve_native_table(
                DEFAULT_CATALOG,
                &self.reference.schema,
                &self.reference.name,
            )
            .ok_or_else(|| ExecError::Internal("Missing table after catalog insert".to_string()))
            .unwrap();

        let table = storage.create_table(ent).await.map_err(|e| {
            DataFusionError::Execution(format!("failed to create table in storage: {e}"))
        })?;
        info!(loc = %table.storage_location(), "native table created");

        let res = if let Some(source) = self.source {
            InsertExec::do_insert(table.into_table_provider(), source, context).await?
        } else {
            RecordBatch::new_empty(schema)
        };

        // TODO: Add storage tracking job.

        Ok(res)
    }
}
