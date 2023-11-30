//! Table functions for triggering system-related functionality. Users are
//! unlikely to use these, but there's no harm if they do.

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{
    FuncParamValue, TableFunc, TableFuncContextProvider, VirtualLister,
};
use datafusion_ext::system::SystemOperation;
use datasources::native::access::{NativeTable, NativeTableStorage, SaveMode};
use futures::{stream, Future, StreamExt};
use protogen::metastore::types::catalog::{CatalogEntry, RuntimePreference, TableEntry};
use protogen::metastore::types::options::DatabaseOptions;
use std::collections::HashMap;
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
                        lister,
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
                "Unexpected entry type for builtin table: {}",
                GLARE_CACHED_EXTERNAL_DATABASE_TABLES.name
            ),
        };

        let op = CacheExternalDatabaseTablesOperation {
            listers: Arc::new(listers),
            table,
        };

        unimplemented!()
    }
}

struct ListerForDatabase {
    /// Oid this lister is for.
    oid: u32,
    lister: Box<dyn VirtualLister>,
}

struct CacheExternalDatabaseTablesOperation {
    /// Virtual listers for external databases we'll be listing from.
    listers: Arc<Vec<ListerForDatabase>>,
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
        let fut = async move {
            let table = storage.load_table(&table).await.unwrap();
            //
            Ok(())
        };

        Box::pin(fut)
    }
}

struct StreamingListerExec {
    listers: Arc<Vec<ListerForDatabase>>,
}
