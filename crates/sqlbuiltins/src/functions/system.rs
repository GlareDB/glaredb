//! Table functions for triggering system-related functionality. Users are
//! unlikely to use these, but there's no harm if they do.

use async_trait::async_trait;
use datafusion::arrow::array::{StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{
    FuncParamValue, TableFunc, TableFuncContextProvider, VirtualLister,
};
use datafusion_ext::system::SystemOperation;
use datasources::native::access::NativeTableStorage;
use futures::{stream, Future, StreamExt};
use protogen::metastore::types::catalog::{CatalogEntry, RuntimePreference, TableEntry};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use tracing::warn;

use crate::builtins::GLARE_CACHED_EXTERNAL_DATABASE_TABLES;
use crate::functions::virtual_listing::get_virtual_lister_for_options;

#[derive(Debug, Clone, Copy)]
pub struct CacheExternalDatabaseTables;

#[async_trait]
impl TableFunc for CacheExternalDatabaseTables {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Remote
    }

    fn name(&self) -> &str {
        "cache_external_database_tables"
    }

    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(0, Vec::new(), Volatility::Stable))
    }

    async fn create_provider(
        &self,
        context: &dyn TableFuncContextProvider,
        _args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        // TODO: Try to reduce clones.
        //
        // TODO: We can allow selectively updating cached tables in the future
        // with something like `cache_external_database_tables(my_db)`.
        // Currently this just does everything.

        let db_ents: Vec<_> = context
            .get_session_catalog()
            .iter_entries()
            .filter_map(|ent| {
                if let CatalogEntry::Database(db_ent) = ent.entry {
                    Some(db_ent)
                } else {
                    None
                }
            })
            .collect();

        let listers: Vec<ListerForDatabase> = stream::iter(db_ents.into_iter())
            .filter_map(|ent| async {
                match get_virtual_lister_for_options(&ent.options).await {
                    Ok(lister) => Some(ListerForDatabase {
                        oid: ent.meta.id,
                        lister: lister.into(),
                    }),
                    Err(e) => {
                        // TODO: We'll want to store errors in a table too so we
                        // can easily present them to the user at a later date
                        // (e.g. "failed to get information for database because
                        // ...").
                        warn!(%e, oid = %ent.meta.id, "failed to get virtual lister for database");
                        None
                    }
                }
            })
            .collect()
            .await;

        let table = match context
            .get_session_catalog()
            .get_by_oid(GLARE_CACHED_EXTERNAL_DATABASE_TABLES.oid)
            .ok_or_else(|| ExtensionError::MissingObject {
                obj_typ: "table",
                name: GLARE_CACHED_EXTERNAL_DATABASE_TABLES.name.to_string(),
            })? {
            CatalogEntry::Table(ent) => ent.clone(),
            other => panic!(
                "Unexpected entry type for builtin table: {}, got: {other:?}",
                GLARE_CACHED_EXTERNAL_DATABASE_TABLES.name
            ),
        };

        let op = CacheExternalDatabaseTablesOperation { listers, table };

        unimplemented!()
        // Ok(Arc::new(SystemOperationTableProvider {
        //     operation: Arc::new(op),
        // }))
    }
}

#[derive(Clone)]
struct ListerForDatabase {
    /// Oid this lister is for.
    oid: u32,
    lister: Arc<dyn VirtualLister>,
}

struct CacheExternalDatabaseTablesOperation {
    /// Virtual listers for external databases we'll be listing from.
    listers: Vec<ListerForDatabase>,
    /// Table we'll be writing the cache to.
    table: TableEntry,
}

impl SystemOperation for CacheExternalDatabaseTablesOperation {
    fn name(&self) -> &'static str {
        "cache_external_database_tables"
    }

    fn create_future(
        &self,
        context: Arc<TaskContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), DataFusionError>> + Send>> {
        let storage = context
            .session_config()
            .get_extension::<NativeTableStorage>()
            .expect("Native table storage to be on context");

        let table = self.table.clone();
        let listers = self.listers.clone();
        let fut = async move {
            let table = storage.load_table(&table).await.unwrap();
            let input_plan = Arc::new(StreamingListerExec { listers });
            // Note we may want to reconsider this in the future if we want to
            // selectively update data sources.
            let exec = table.insert_exec(input_plan, true);
            let mut stream = exec.execute(0, context)?;

            // Execute stream to completion
            while let Some(result) = stream.next().await {
                let _ = result?;
            }

            Ok(())
        };

        Box::pin(fut)
    }
}

/// Input execution plan for the table write to avoid buffering everything from
/// the lister in memory.
///
/// The streaming output is partitioned by external database.
struct StreamingListerExec {
    listers: Vec<ListerForDatabase>,
}

impl ExecutionPlan for StreamingListerExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(GLARE_CACHED_EXTERNAL_DATABASE_TABLES.arrow_schema())
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.listers.len())
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
            "Cannot change children for StreamingListerExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let arrow_schema = self.schema();
        let lister = self.listers[partition].clone();

        let out = stream::once(async move {
            let mut oid_arr = UInt32Builder::new();
            let mut schema_name_arr = StringBuilder::new();
            let mut table_name_arr = StringBuilder::new();

            let schemas = lister
                .lister
                .list_schemas()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            for schema in schemas {
                let tables = lister
                    .lister
                    .list_tables(&schema)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                for table in tables {
                    oid_arr.append_value(lister.oid);
                    schema_name_arr.append_value(&schema);
                    table_name_arr.append_value(table);
                }
            }

            let oid_arr = oid_arr.finish();
            let schema_name_arr = schema_name_arr.finish();
            let table_name_arr = table_name_arr.finish();

            Ok(RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(oid_arr),
                    Arc::new(schema_name_arr),
                    Arc::new(table_name_arr),
                ],
            )
            .unwrap())
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), out)))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for StreamingListerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamingListerExec")
    }
}

impl fmt::Debug for StreamingListerExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingListerExec").finish()
    }
}
