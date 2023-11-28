use std::sync::Arc;

use catalog::session_catalog::{CatalogMutator, SessionCatalog};
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec,
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use datasources::native::access::{NativeTable, NativeTableStorage, SaveMode};
use futures::stream;
use protogen::metastore::types::{service, service::Mutation};
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use tracing::debug;

use super::GENERIC_OPERATION_PHYSICAL_SCHEMA;
use crate::{
    errors::ExecError,
    planner::{logical_plan::OwnedFullObjectReference, physical_plan::new_operation_batch},
};
use futures::StreamExt;

#[derive(Debug, Clone)]
pub struct CreateTableExec {
    pub catalog_version: u64,
    pub tbl_reference: OwnedFullObjectReference,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub arrow_schema: SchemaRef,
    pub source: Option<Arc<dyn ExecutionPlan>>,
}

impl ExecutionPlan for CreateTableExec {
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
            tbl_reference: self.tbl_reference.clone(),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
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
        let or_replace = self.or_replace;
        let if_not_exists = self.if_not_exists;

        let state = mutator
            .mutate(
                self.catalog_version,
                [Mutation::CreateTable(service::CreateTable {
                    schema: self.tbl_reference.schema.clone().into_owned(),
                    name: self.tbl_reference.name.clone().into_owned(),
                    options: self.arrow_schema.into(),
                    if_not_exists,
                    or_replace,
                })],
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("failed to create table in catalog: {e}"))
            })?;

        let source = self.source.map(|source| {
            if source.output_partitioning().partition_count() != 1 {
                Arc::new(CoalescePartitionsExec::new(source))
            } else {
                source
            }
        });

        // Note that we're not changing out the catalog stored on the context,
        // we're just using it here to get the new table entry easily.
        let new_catalog = SessionCatalog::new(state);

        let ent = new_catalog
            .resolve_table(
                DEFAULT_CATALOG,
                &self.tbl_reference.schema,
                &self.tbl_reference.name,
            )
            .ok_or_else(|| ExecError::Internal("Missing table after catalog insert".to_string()))
            .unwrap();

        let save_mode = match (if_not_exists, or_replace) {
            (true, false) => SaveMode::Ignore,
            (false, true) => SaveMode::Overwrite,
            (false, false) => SaveMode::ErrorIfExists,
            (true, true) => {
                return Err(DataFusionError::Internal(
                    "cannot create table with both `if_not_exists` and `or_replace` policies"
                        .to_string(),
                ))
            }
        };

        let table = storage.create_table(ent, save_mode).await.map_err(|e| {
            DataFusionError::Execution(format!("failed to create table in storage: {e}"))
        })?;

        match (source, or_replace) {
            (Some(input), overwrite) => insert(&table, input, overwrite, context).await?,

            // if it's a 'replace' and there is no insert, we overwrite with an empty table
            (None, true) => {
                let input = Arc::new(EmptyExec::new(false, TableProvider::schema(&table)));
                insert(&table, input, true, context).await?
            }
            (None, false) => {}
        };
        debug!(loc = %table.storage_location(), "native table created");

        // TODO: Add storage tracking job.

        Ok(new_operation_batch("create_table"))
    }
}

async fn insert(
    tbl: &NativeTable,
    input: Arc<dyn ExecutionPlan>,
    overwrite: bool,
    context: Arc<TaskContext>,
) -> DataFusionResult<()> {
    let mut stream = tbl.insert_exec(input, overwrite).execute(0, context)?;

    while let Some(res) = stream.next().await {
        // Drain stream to write everything.
        let _ = res?;
    }
    Ok(())
}
