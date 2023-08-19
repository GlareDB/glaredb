//! Various table dispatchers.
pub mod external;
pub mod system;

use std::sync::Arc;

use datafusion::{datasource::TableProvider, prelude::SessionContext};
use datasources::native::access::NativeTableStorage;
use protogen::metastore::types::catalog::{CatalogEntry, EntryMeta, EntryType, ViewEntry};
use sqlbuiltins::builtins::{CURRENT_SESSION_SCHEMA, DEFAULT_CATALOG};

use crate::{
    dispatch::system::SystemTableDispatcher,
    metastore::catalog::{SessionCatalog, TempObjects},
    metrics::SessionMetrics,
};

use self::external::ExternalDispatcher;

type Result<T, E = DispatchError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("Missing database: {database}")]
    MissingDatabase { database: String },

    #[error("Missing catalog entry; schema: {schema}, name: {name}")]
    MissingEntry { schema: String, name: String },

    #[error("Missing builtin table; schema: {schema}, name: {name}")]
    MissingBuiltinTable { schema: String, name: String },

    #[error("Missing object with oid: {0}")]
    MissingObjectWithOid(u32),

    #[error("Missing tunnel connection: {0}")]
    MissingTunnel(u32),

    #[error("Invalid entry for table dispatch: {0}")]
    InvalidEntryTypeForDispatch(EntryType),

    #[error("Unhandled entry for table dispatch: {0:?}")]
    UnhandledEntry(EntryMeta),

    #[error("failed to do late planning: {0}")]
    LatePlanning(Box<crate::planner::errors::PlanError>),

    #[error("Invalid dispatch: {0}")]
    InvalidDispatch(&'static str),

    #[error(transparent)]
    RemoteDispatch(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    DebugDatasource(#[from] datasources::debug::errors::DebugError),
    #[error(transparent)]
    PostgresDatasource(#[from] datasources::postgres::errors::PostgresError),
    #[error(transparent)]
    BigQueryDatasource(#[from] datasources::bigquery::errors::BigQueryError),
    #[error(transparent)]
    MysqlDatasource(#[from] datasources::mysql::errors::MysqlError),
    #[error(transparent)]
    ObjectStoreDatasource(#[from] datasources::object_store::errors::ObjectStoreSourceError),
    #[error(transparent)]
    MongoDatasource(#[from] datasources::mongodb::errors::MongoError),
    #[error(transparent)]
    SnowflakeDatasource(#[from] datasources::snowflake::errors::DatasourceSnowflakeError),
    #[error(transparent)]
    DeltaDatasource(#[from] datasources::lake::delta::errors::DeltaError),
    #[error(transparent)]
    NativeDatasource(#[from] datasources::native::errors::NativeError),
    #[error(transparent)]
    CommonDatasource(#[from] datasources::common::errors::DatasourceCommonError),
    #[error(transparent)]
    SshKey(#[from] datasources::common::ssh::key::SshKeyError),
}

impl DispatchError {
    /// Whether or not this error should indicate to the planner to try looking
    /// in a different schema for the requested object.
    ///
    /// For example, if a user's search path is '[public, other]', and 'table_a'
    /// exists in 'other', then the dispatch will fail the first time with
    /// `MissingEntry` since it will look for 'public.table_a' first. In such
    /// cases, we should try the next schema.
    pub fn should_try_next_schema(&self) -> bool {
        matches!(
            self,
            DispatchError::MissingEntry { .. } | DispatchError::InvalidEntryTypeForDispatch(_)
        )
    }
}

pub struct Dispatcher<'a> {
    catalog: &'a SessionCatalog,
    tables: &'a NativeTableStorage,
    metrics: &'a SessionMetrics,
    temp_objects: &'a TempObjects,
    // TODO: Remove need for this.
    df_ctx: &'a SessionContext,
    /// Whether or not local file system access should be disabled.
    disable_local_fs_access: bool,
}

impl<'a> Dispatcher<'a> {
    pub fn new(
        catalog: &'a SessionCatalog,
        tables: &'a NativeTableStorage,
        metrics: &'a SessionMetrics,
        temp_objects: &'a TempObjects,
        df_ctx: &'a SessionContext,
        disable_local_fs_access: bool,
    ) -> Self {
        Dispatcher {
            catalog,
            tables,
            metrics,
            temp_objects,
            df_ctx,
            disable_local_fs_access,
        }
    }

    /// Dispatch to a table provider.
    pub async fn dispatch(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        // "External" database.
        if database != DEFAULT_CATALOG {
            let db = match self.catalog.resolve_database(database) {
                Some(db) => db,
                None => {
                    return Err(DispatchError::MissingDatabase {
                        database: database.to_string(),
                    })
                }
            };
            return ExternalDispatcher::new(
                self.catalog,
                self.df_ctx,
                self.disable_local_fs_access,
            )
            .dispatch_external_database(db, schema, name)
            .await;
        }

        if schema == CURRENT_SESSION_SCHEMA {
            return match self.temp_objects.resolve_temp_table(name) {
                Some(table) => Ok(table),
                None => Err(DispatchError::MissingEntry {
                    schema: schema.to_owned(),
                    name: name.to_owned(),
                }),
            };
        }

        let ent = self
            .catalog
            .resolve_entry(database, schema, name)
            .ok_or_else(|| DispatchError::MissingEntry {
                schema: schema.to_string(),
                name: name.to_string(),
            })?;

        // Only allow dispatching to types we can actually convert to a table
        // provider.
        if !matches!(ent.entry_type(), EntryType::View | EntryType::Table) {
            return Err(DispatchError::InvalidEntryTypeForDispatch(ent.entry_type()));
        }

        match ent {
            CatalogEntry::View(view) => self.dispatch_view(view).await,
            // Dispatch to builtin tables.
            CatalogEntry::Table(tbl) if tbl.meta.builtin => {
                SystemTableDispatcher::new(self.catalog, self.metrics).dispatch(schema, name)
            }
            // Dispatch to external tables.
            CatalogEntry::Table(tbl) if tbl.meta.external => {
                ExternalDispatcher::new(self.catalog, self.df_ctx, self.disable_local_fs_access)
                    .dispatch_external_table(tbl)
                    .await
            }
            // Dispatch to native tables.
            CatalogEntry::Table(tbl) => {
                let table = self.tables.load_table(tbl).await?;
                Ok(table.into_table_provider())
            }
            other => Err(DispatchError::UnhandledEntry(other.get_meta().clone())),
        }
    }

    async fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        unimplemented!()
    }
}
