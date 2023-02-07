//! Adapter types for dispatching to table sources.
use crate::context::SessionContext;
use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt32Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datasource_bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasource_common::ssh::SshKey;
use datasource_debug::DebugTableType;
use datasource_mysql::{MysqlAccessor, MysqlTableAccess};
use datasource_object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasource_object_store::local::{LocalAccessor, LocalTableAccess};
use datasource_object_store::s3::{S3Accessor, S3TableAccess};
use datasource_postgres::{PostgresAccessor, PostgresTableAccess};
use metastore::builtins::{
    GLARE_COLUMNS, GLARE_CONNECTIONS, GLARE_SCHEMAS, GLARE_SSH_CONNECTIONS, GLARE_TABLES,
    GLARE_VIEWS,
};
use metastore::session::SessionCatalog;
use metastore::types::catalog::{
    CatalogEntry, ConnectionEntry, ConnectionOptions, ConnectionOptionsSsh, EntryType,
    ExternalTableEntry, TableOptions, ViewEntry,
};
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task;
use tracing::error;

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("Missing catalog entry; schema: {schema}, name: {name}")]
    MissingEntry { schema: String, name: String },

    #[error("Missing builtin table; schema: {schema}, name: {name}")]
    MissingBuiltinTable { schema: String, name: String },

    #[error("Missing object with oid: {0}")]
    MissingObjectWithOid(u32),

    #[error("Unable to retrieve ssh tunnel connection: {0}")]
    MissingSshTunnel(Box<crate::errors::ExecError>),

    #[error("Invalid entry for table dispatch: {0}")]
    InvalidEntryTypeForDispatch(EntryType),

    #[error("Unhandled entry for table dispatch: {0}")]
    UnhandledEntryType(EntryType),

    #[error("failed to do late planning: {0}")]
    LatePlanning(Box<crate::errors::ExecError>),

    #[error(
        "Unhandled external dispatch; table type: {table_type}, connection type: {connection_type}"
    )]
    UnhandledExternalDispatch {
        table_type: String,
        connection_type: String,
    },

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    DebugDatasource(#[from] datasource_debug::errors::DebugError),
    #[error(transparent)]
    PostgresDatasource(#[from] datasource_postgres::errors::PostgresError),
    #[error(transparent)]
    BigQueryDatasource(#[from] datasource_bigquery::errors::BigQueryError),
    #[error(transparent)]
    MysqlDatasource(#[from] datasource_mysql::errors::MysqlError),
    #[error(transparent)]
    ObjectStoreDatasource(#[from] datasource_object_store::errors::ObjectStoreSourceError),
    #[error(transparent)]
    CommonDatasource(#[from] datasource_common::errors::Error),
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

type Result<T, E = DispatchError> = std::result::Result<T, E>;

/// Dispatch to the appropriate table sources.
pub struct SessionDispatcher<'a> {
    ctx: &'a SessionContext,
}

impl<'a> SessionDispatcher<'a> {
    /// Create a new dispatcher.
    pub fn new(ctx: &'a SessionContext) -> Self {
        SessionDispatcher { ctx }
    }

    /// Return a datafusion table provider based on how the table should be
    /// accessed.
    pub fn dispatch_access(&self, schema: &str, name: &str) -> Result<Arc<dyn TableProvider>> {
        let catalog = self.ctx.get_session_catalog();

        let ent =
            catalog
                .resolve_entry(schema, name)
                .ok_or_else(|| DispatchError::MissingEntry {
                    schema: schema.to_string(),
                    name: name.to_string(),
                })?;

        // Only allow dispatching to types we can actually convert to a table
        // provider.
        if !matches!(
            ent.entry_type(),
            EntryType::View | EntryType::Table | EntryType::ExternalTable
        ) {
            return Err(DispatchError::InvalidEntryTypeForDispatch(ent.entry_type()));
        }

        // Dispatch to system tables if builtin...
        if ent.get_meta().builtin && ent.entry_type() == EntryType::Table {
            let table = SystemTableDispatcher::new(catalog).dispatch(schema, name)?;
            return Ok(table);
        }

        match ent {
            CatalogEntry::View(view) => self.dispatch_view(view),
            CatalogEntry::ExternalTable(table) => self.dispatch_external_table(table),
            // Note that all 'table' entries should have already been handled
            // with the above builtin table dispatcher.
            other => Err(DispatchError::UnhandledEntryType(other.entry_type())),
        }
    }

    fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        let plan = self
            .ctx
            .late_view_plan(&view.sql)
            .map_err(|e| DispatchError::LatePlanning(Box::new(e)))?;
        Ok(Arc::new(ViewTable::try_new(plan, None)?))
    }

    fn dispatch_external_table(
        &self,
        table: &ExternalTableEntry,
    ) -> Result<Arc<dyn TableProvider>> {
        let conn = self
            .ctx
            .get_session_catalog()
            .get_by_oid(table.connection_id)
            .ok_or(DispatchError::MissingObjectWithOid(table.connection_id))?;
        let conn = match conn {
            CatalogEntry::Connection(conn) => conn,
            other => {
                return Err(DispatchError::InvalidEntryTypeForDispatch(
                    other.entry_type(),
                ))
            }
        };

        match (&conn.options, &table.options) {
            (ConnectionOptions::Debug(_), TableOptions::Debug(table)) => {
                let provider = DebugTableType::from_str(&table.table_type)?;
                Ok(provider.into_table_provider())
            }
            (ConnectionOptions::Postgres(conn), TableOptions::Postgres(table)) => {
                let table_access = PostgresTableAccess {
                    schema: table.schema.clone(),
                    name: table.table.clone(),
                    connection_string: conn.connection_string.clone(),
                };

                let ssh_tunnel_access = self
                    .ctx
                    .get_ssh_tunnel_access(conn.ssh_tunnel.to_owned())
                    .map_err(|e| DispatchError::MissingSshTunnel(Box::new(e)))?;

                let predicate_pushdown = *self
                    .ctx
                    .get_session_vars()
                    .postgres_predicate_pushdown
                    .value();
                let result: Result<_, datasource_postgres::errors::PostgresError> =
                    task::block_in_place(move || {
                        Handle::current().block_on(async move {
                            let accessor =
                                PostgresAccessor::connect(table_access, ssh_tunnel_access).await?;
                            let provider = accessor.into_table_provider(predicate_pushdown).await?;
                            Ok(provider)
                        })
                    });
                let provider = result?;
                Ok(Arc::new(provider))
            }
            (ConnectionOptions::BigQuery(conn), TableOptions::BigQuery(table)) => {
                let table_access = BigQueryTableAccess {
                    gcp_service_acccount_key_json: conn.service_account_key.clone(),
                    gcp_project_id: conn.project_id.clone(),
                    dataset_id: table.dataset_id.clone(),
                    table_id: table.table_id.clone(),
                };
                let predicate_pushdown = *self
                    .ctx
                    .get_session_vars()
                    .bigquery_predicate_pushdown
                    .value();
                let result: Result<_, datasource_bigquery::errors::BigQueryError> =
                    task::block_in_place(move || {
                        Handle::current().block_on(async move {
                            let accessor = BigQueryAccessor::connect(table_access).await?;
                            let provider = accessor.into_table_provider(predicate_pushdown).await?;
                            Ok(provider)
                        })
                    });
                let provider = result?;
                Ok(Arc::new(provider))
            }
            (ConnectionOptions::Mysql(conn), TableOptions::Mysql(table)) => {
                let table_access = MysqlTableAccess {
                    schema: table.schema.clone(),
                    name: table.table.clone(),
                    connection_string: conn.connection_string.clone(),
                };

                let ssh_tunnel_access = self
                    .ctx
                    .get_ssh_tunnel_access(conn.ssh_tunnel.to_owned())
                    .map_err(|e| DispatchError::MissingSshTunnel(Box::new(e)))?;

                let predicate_pushdown = *self
                    .ctx
                    .get_session_vars()
                    .postgres_predicate_pushdown
                    .value();
                let result: Result<_, datasource_mysql::errors::MysqlError> =
                    task::block_in_place(move || {
                        Handle::current().block_on(async move {
                            let accessor =
                                MysqlAccessor::connect(table_access, ssh_tunnel_access).await?;
                            let provider = accessor.into_table_provider(predicate_pushdown).await?;
                            Ok(provider)
                        })
                    });
                let provider = result?;
                Ok(Arc::new(provider))
            }
            (ConnectionOptions::Local(_), TableOptions::Local(table)) => {
                let table_access = LocalTableAccess {
                    location: table.location.clone(),
                };
                let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
                    task::block_in_place(move || {
                        Handle::current().block_on(async move {
                            let accessor = LocalAccessor::new(table_access).await?;
                            let provider = accessor.into_table_provider(true).await?;
                            Ok(provider)
                        })
                    });
                let provider = result?;
                Ok(Arc::new(provider))
            }
            (ConnectionOptions::Gcs(conn), TableOptions::Gcs(table)) => {
                let table_access = GcsTableAccess {
                    service_acccount_key_json: conn.service_account_key.clone(),
                    bucket_name: table.bucket_name.clone(),
                    location: table.location.clone(),
                };
                let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
                    task::block_in_place(move || {
                        Handle::current().block_on(async move {
                            let accessor = GcsAccessor::new(table_access).await?;
                            let provider = accessor.into_table_provider(true).await?;
                            Ok(provider)
                        })
                    });
                let provider = result?;
                Ok(Arc::new(provider))
            }
            (ConnectionOptions::S3(conn), TableOptions::S3(table)) => {
                let table_access = S3TableAccess {
                    region: table.region.clone(),
                    bucket_name: table.bucket_name.clone(),
                    location: table.location.clone(),
                    access_key_id: conn.access_key_id.clone(),
                    secret_access_key: conn.access_key_secret.clone(),
                };
                let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
                    task::block_in_place(move || {
                        Handle::current().block_on(async move {
                            let accessor = S3Accessor::new(table_access).await?;
                            let provider = accessor.into_table_provider(true).await?;
                            Ok(provider)
                        })
                    });
                let provider = result?;
                Ok(Arc::new(provider))
            }
            (conn, table) => Err(DispatchError::UnhandledExternalDispatch {
                table_type: table.to_string(),
                connection_type: conn.to_string(),
            }),
        }
    }
}

/// Dispatch to builtin system tables.
struct SystemTableDispatcher<'a> {
    catalog: &'a SessionCatalog,
}

impl<'a> SystemTableDispatcher<'a> {
    fn new(catalog: &'a SessionCatalog) -> Self {
        SystemTableDispatcher { catalog }
    }

    fn dispatch(&self, schema: &str, name: &str) -> Result<Arc<dyn TableProvider>> {
        Ok(if GLARE_TABLES.matches(schema, name) {
            Arc::new(self.build_glare_tables())
        } else if GLARE_COLUMNS.matches(schema, name) {
            Arc::new(self.build_glare_columns())
        } else if GLARE_VIEWS.matches(schema, name) {
            Arc::new(self.build_glare_views())
        } else if GLARE_SCHEMAS.matches(schema, name) {
            Arc::new(self.build_glare_schemas())
        } else if GLARE_CONNECTIONS.matches(schema, name) {
            Arc::new(self.build_glare_connections())
        } else if GLARE_SSH_CONNECTIONS.matches(schema, name) {
            Arc::new(self.build_glare_ssh_connections())
        } else {
            return Err(DispatchError::MissingBuiltinTable {
                schema: schema.to_string(),
                name: name.to_string(),
            });
        })
    }

    fn build_glare_schemas(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_SCHEMAS.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();

        for schema in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Schema)
        {
            oids.append_value(schema.oid);
            builtins.append_value(schema.builtin);
            schema_names.append_value(&schema.entry.get_meta().name);
        }
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_tables(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TABLES.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_oids = UInt32Builder::new();
        let mut schema_names = StringBuilder::new();
        let mut table_names = StringBuilder::new();
        let mut externals = BooleanBuilder::new();
        let mut connection_oids = UInt32Builder::new();

        for table in self.catalog.iter_entries().filter(|ent| {
            ent.entry_type() == EntryType::Table || ent.entry_type() == EntryType::ExternalTable
        }) {
            oids.append_value(table.oid);
            builtins.append_value(table.builtin);
            schema_oids.append_value(
                table
                    .schema_entry
                    .map(|schema| schema.get_meta().id)
                    .unwrap_or(0),
            );
            schema_names.append_value(
                table
                    .schema_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            table_names.append_value(&table.entry.get_meta().name);
            externals.append_value(table.entry_type() == EntryType::ExternalTable);

            match table.entry {
                CatalogEntry::ExternalTable(ent) => connection_oids.append_value(ent.connection_id),
                _ => connection_oids.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_oids.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(externals.finish()),
                Arc::new(connection_oids.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_columns(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_COLUMNS.arrow_schema());

        let mut table_oids = UInt32Builder::new();
        let mut schema_names = StringBuilder::new();
        let mut table_names = StringBuilder::new();
        let mut column_names = StringBuilder::new();
        let mut column_indexes = UInt32Builder::new();
        let mut data_types = StringBuilder::new();
        let mut is_nullables = BooleanBuilder::new();

        for table in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Table)
        {
            let ent = match table.entry {
                CatalogEntry::Table(ent) => ent,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            for (i, col) in ent.columns.iter().enumerate() {
                table_oids.append_value(table.oid);
                schema_names.append_value(
                    table
                        .schema_entry
                        .map(|schema| schema.get_meta().name.as_str())
                        .unwrap_or("<invalid>"),
                );
                table_names.append_value(&table.entry.get_meta().name);
                column_names.append_value(&col.name);
                column_indexes.append_value(i as u32);
                data_types.append_value(col.arrow_type.to_string());
                is_nullables.append_value(col.nullable);
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(table_oids.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(column_names.finish()),
                Arc::new(column_indexes.finish()),
                Arc::new(data_types.finish()),
                Arc::new(is_nullables.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_views(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_VIEWS.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();
        let mut view_names = StringBuilder::new();
        let mut sqls = StringBuilder::new();

        for view in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::View)
        {
            let ent = match view.entry {
                CatalogEntry::View(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oids.append_value(view.oid);
            builtins.append_value(view.builtin);
            schema_names.append_value(
                view.schema_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            view_names.append_value(&view.entry.get_meta().name);
            sqls.append_value(&ent.sql);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(view_names.finish()),
                Arc::new(sqls.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_connections(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_CONNECTIONS.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();
        let mut connection_names = StringBuilder::new();
        let mut connection_types = StringBuilder::new();

        for conn in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Connection)
        {
            let ent = match conn.entry {
                CatalogEntry::Connection(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oids.append_value(conn.oid);
            builtins.append_value(conn.builtin);
            schema_names.append_value(
                conn.schema_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            connection_names.append_value(&conn.entry.get_meta().name);
            connection_types.append_value(ent.options.to_string());
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(connection_names.finish()),
                Arc::new(connection_types.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_ssh_connections(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_SSH_CONNECTIONS.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();
        let mut connection_names = StringBuilder::new();
        let mut public_key = StringBuilder::new();

        for conn in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Connection)
        {
            match conn.entry {
                CatalogEntry::Connection(ConnectionEntry {
                    options: ConnectionOptions::Ssh(ConnectionOptionsSsh { keypair, .. }),
                    ..
                }) => {
                    let ssh_public_key = SshKey::from_bytes(keypair)
                        .expect("Keypair should always be a valid ssh key")
                        .public_key()
                        .expect("Always produces a valid OpenSSH public key");

                    oids.append_value(conn.oid);
                    builtins.append_value(conn.builtin);
                    schema_names.append_value(
                        conn.schema_entry
                            .map(|schema| schema.get_meta().name.as_str())
                            .unwrap_or("<invalid>"),
                    );
                    connection_names.append_value(&conn.entry.get_meta().name);
                    public_key.append_value(ssh_public_key);
                }
                CatalogEntry::Connection(_) => (),
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(connection_names.finish()),
                Arc::new(public_key.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }
}
