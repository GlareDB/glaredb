use crate::functions::table::TableFunc;
use crate::functions::ConstBuiltinFunction;
use async_trait::async_trait;
use datafusion::arrow::array::{StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider, VirtualLister};
use datafusion_ext::system::SystemOperation;
use datasources::native::access::{NativeTableStorage, SaveMode};
use futures::{stream, Future, StreamExt};
use protogen::metastore::types::catalog::FunctionType;
use protogen::metastore::types::catalog::{CatalogEntry, RuntimePreference, TableEntry};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use tracing::warn;

use super::SystemOperationTableProvider;
use crate::builtins::GLARE_CACHED_EXTERNAL_DATABASE_TABLES;
use crate::functions::table::virtual_listing::get_virtual_lister_for_external_db;

#[derive(Debug, Clone, Copy)]
pub struct CacheExternalDatabaseTables;

impl ConstBuiltinFunction for CacheExternalDatabaseTables {
    const NAME: &'static str = "cache_external_database_tables";
    const DESCRIPTION: &'static str = "Cache tables from external databases.";
    const EXAMPLE: &'static str = "select * from cache_external_database_tables();";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for CacheExternalDatabaseTables {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Remote
    }

    async fn create_provider(
        &self,
        context: &dyn TableFuncContextProvider,
        _args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        // TODO: We can allow selectively updating cached tables in the future
        // with something like `cache_external_database_tables(my_db)`.
        // Currently this just does everything.

        let external_db_ents: Vec<_> = context
            .get_session_catalog()
            .iter_entries()
            .filter_map(|ent| match ent.entry {
                CatalogEntry::Database(db_ent) if db_ent.meta.external => Some(db_ent),
                _ => None,
            })
            .collect();

        let listers: Vec<ListerForDatabase> = stream::iter(external_db_ents.into_iter())
            .filter_map(|ent| async {
                match get_virtual_lister_for_external_db(&ent.options).await {
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

        Ok(Arc::new(SystemOperationTableProvider {
            operation: Arc::new(op),
        }))
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
            // Note we may want to reconsider this in the future if we want to
            // selectively update data sources. Currently this overwrites the
            // entire table everytime.

            let table = storage
                .create_table(&table, SaveMode::Overwrite)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let input_plan = Arc::new(StreamingListerExec { listers });
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
            let mut column_name_arr = StringBuilder::new();
            let mut data_type_arr = StringBuilder::new();

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
                    let cols = match lister.lister.list_columns(&schema, &table).await {
                        Ok(cols) => cols,
                        Err(e) => {
                            // This gets us past potentially unsupported
                            // types. We don't want to fail everything if
                            // one table has something we don't expect.
                            //
                            // Eventually we'll want to store this error
                            // somewhere so the user knows that we weren't able
                            // to get the cols for table and why.
                            warn!(%schema, %table, %e, "failed to get columns for external database table");
                            continue;
                        }
                    };

                    for col in cols.into_iter() {
                        oid_arr.append_value(lister.oid);
                        schema_name_arr.append_value(&schema);
                        table_name_arr.append_value(&table);
                        column_name_arr.append_value(col.name());
                        data_type_arr.append_value(col.data_type().to_string());
                    }
                }
            }

            let oid_arr = oid_arr.finish();
            let schema_name_arr = schema_name_arr.finish();
            let table_name_arr = table_name_arr.finish();
            let column_name_arr = column_name_arr.finish();
            let data_type_arr = data_type_arr.finish();

            Ok(RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(oid_arr),
                    Arc::new(schema_name_arr),
                    Arc::new(table_name_arr),
                    Arc::new(column_name_arr),
                    Arc::new(data_type_arr),
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
