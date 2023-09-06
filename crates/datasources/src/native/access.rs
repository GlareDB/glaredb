use crate::native::errors::{NativeError, Result};
use crate::native::insert::NativeTableInsertExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::Expr;
use deltalake::action::SaveMode;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::delete::DeleteBuilder;
use deltalake::operations::update::UpdateBuilder;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaTable, DeltaTableConfig};
use futures::StreamExt;
use object_store::path::Path as ObjectStorePath;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use object_store_util::{conf::StorageConfig, shared::SharedObjectStore};
use protogen::metastore::types::catalog::TableEntry;
use protogen::metastore::types::options::{
    InternalColumnDefinition, TableOptions, TableOptionsInternal,
};
use std::any::Any;
use std::sync::Arc;
use tokio::fs;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NativeTableStorage {
    db_id: Uuid,
    conf: StorageConfig,

    /// Tables are only located in one bucket which the provided service account
    /// should have access to.
    ///
    /// Delta-rs expects that the root of the object store client points to the
    /// table location. We can use `PrefixStore` for that, but we need something
    /// that implements `ObjectStore`. That's what this type is for.
    ///
    /// Arcs all the way down...
    store: SharedObjectStore,
}

impl NativeTableStorage {
    /// Create a native table storage provider from the given config.
    pub fn from_config(db_id: Uuid, conf: StorageConfig) -> Result<NativeTableStorage> {
        let store = conf.new_object_store()?;
        Ok(NativeTableStorage {
            db_id,
            conf,
            store: SharedObjectStore::new(store),
        })
    }

    /// Returns the database ID.
    pub fn db_id(&self) -> Uuid {
        self.db_id
    }

    /// Calculates the total size of storage being used by the database in
    /// bytes.
    pub async fn calculate_db_size(&self) -> Result<usize> {
        let prefix: ObjectStorePath = format!("databases/{}/", self.db_id).into();
        let mut objects = self.store.list(Some(&prefix)).await?;

        let mut total_size = 0;
        while let Some(meta) = objects.next().await {
            let meta = meta?;
            total_size += meta.size;
        }

        Ok(total_size)
    }

    pub async fn create_table(&self, table: &TableEntry) -> Result<NativeTable> {
        let delta_store = self.create_delta_store_for_table(table).await?;

        let opts = Self::opts_from_ent(table)?;
        let mut builder = CreateBuilder::new()
            .with_table_name(&table.meta.name)
            .with_object_store(delta_store)
            .with_save_mode(SaveMode::ErrorIfExists);
        for col in &opts.columns {
            let column = match col.arrow_type.clone() {
                DataType::Timestamp(_, tz) => InternalColumnDefinition {
                    name: col.name.clone(),
                    nullable: col.nullable,
                    arrow_type: DataType::Timestamp(TimeUnit::Microsecond, tz),
                },
                _ => col.to_owned(),
            };
            builder = builder.with_column(
                column.name.clone(),
                (&column.arrow_type).try_into()?,
                column.nullable,
                None,
            );
        }

        // TODO: Partitioning

        let table = builder.await?;

        Ok(NativeTable::new(table))
    }

    /// Load a native table.
    ///
    /// Errors if the table is not the correct type.
    pub async fn load_table(&self, table: &TableEntry) -> Result<NativeTable> {
        let _ = Self::opts_from_ent(table)?; // Check that this is the correct table type.

        let delta_store = self.create_delta_store_for_table(table).await?;
        let mut table = DeltaTable::new(
            delta_store,
            DeltaTableConfig {
                require_tombstones: true,
                require_files: true,
            },
        );

        table.load().await?;

        Ok(NativeTable::new(table))
    }

    pub async fn delete_table(&self, table: &TableEntry) -> Result<()> {
        let prefix = format!("databases/{}/tables/{}", self.db_id, table.meta.id);
        let path: ObjectStorePath = match &self.conf {
            StorageConfig::Gcs { bucket, .. } => format!("gs://{}/{}", bucket, prefix).into(),
            StorageConfig::Memory => format!("memory://{}", prefix).into(),
            _ => prefix.into(),
        };
        let mut x = self.store.list(Some(&path)).await?;
        while let Some(meta) = x.next().await {
            let meta = meta?;
            self.store.delete(&meta.location).await?
        }
        Ok(())
    }

    fn opts_from_ent(table: &TableEntry) -> Result<&TableOptionsInternal> {
        let opts = match &table.options {
            TableOptions::Internal(opts) => opts,
            _ => return Err(NativeError::NotNative(table.clone())),
        };
        Ok(opts)
    }

    async fn create_delta_store_for_table(
        &self,
        table: &TableEntry,
    ) -> Result<Arc<DeltaObjectStore>> {
        let prefix = format!("databases/{}/tables/{}", self.db_id, table.meta.id);

        let url = match &self.conf {
            StorageConfig::Gcs { bucket, .. } => {
                Url::parse(&format!("gs://{}/{}", bucket, prefix.clone()))?
            }
            StorageConfig::Local { path } => {
                let path =
                    fs::canonicalize(path)
                        .await
                        .map_err(|e| NativeError::CanonicalizePath {
                            path: path.clone(),
                            e,
                        })?;
                let path = path.join(prefix.clone());
                Url::from_file_path(path).map_err(|_| NativeError::Static("Path not absolute"))?
            }
            StorageConfig::Memory => {
                let s = format!("memory://{}", prefix.clone());
                Url::parse(&s)?
            }
        };

        let prefixed = PrefixStore::new(self.store.clone(), prefix);

        let delta_store = DeltaObjectStore::new(Arc::new(prefixed), url);
        Ok(Arc::new(delta_store))
    }

    pub async fn delete_rows_where(
        &self,
        table_entry: &TableEntry,
        where_expr: Option<Expr>,
    ) -> Result<usize> {
        let table = self.load_table(table_entry).await?;
        if let Some(where_expr) = where_expr {
            let deleted_rows = DeleteBuilder::new(table.delta.object_store(), table.delta.state)
                .with_predicate(where_expr)
                .await?
                .1
                .num_deleted_rows;
            Ok(deleted_rows.unwrap_or_default())
        } else {
            let mut records: usize = 0;
            let stats = table.statistics();
            if let Some(stats) = stats {
                let num_rows = stats.num_rows;
                if let Some(num_rows) = num_rows {
                    records = num_rows;
                }
            }
            DeleteBuilder::new(table.delta.object_store(), table.delta.state).await?;
            Ok(records)
        }
    }

    pub async fn update_rows_where(
        &self,
        table: &TableEntry,
        updates: Vec<(String, Expr)>,
        where_expr: Option<Expr>,
    ) -> Result<usize> {
        let table = self.load_table(table).await?;
        let mut builder = UpdateBuilder::new(table.delta.object_store(), table.delta.state);
        for update in updates.into_iter() {
            builder = builder.with_update(update.0, update.1);
        }
        if let Some(where_expr) = where_expr {
            builder = builder.with_predicate(where_expr);
        }
        let updated_rows = builder.await?.1.num_updated_rows;
        Ok(updated_rows)
    }
}

#[derive(Debug)]
pub struct NativeTable {
    delta: DeltaTable,
}

impl NativeTable {
    fn new(delta: DeltaTable) -> Self {
        NativeTable { delta }
    }

    pub fn storage_location(&self) -> String {
        self.delta.table_uri()
    }

    pub fn into_table_provider(self) -> Arc<dyn TableProvider> {
        Arc::new(self)
    }

    /// Create a new execution plan for inserting `input` into the table.
    pub fn insert_exec(&self, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let store = self.delta.object_store();
        let snapshot = self.delta.state.clone();
        Arc::new(NativeTableInsertExec::new(input, store, snapshot))
    }
}

#[async_trait]
impl TableProvider for NativeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        TableProvider::schema(&self.delta)
    }

    fn table_type(&self) -> TableType {
        self.delta.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.delta.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.delta.get_logical_plan()
    }

    async fn scan(
        &self,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let stats = self
            .statistics()
            .unwrap_or_default()
            .num_rows
            .unwrap_or_default();
        if stats == 0 {
            let schema = TableProvider::schema(&self.delta);
            Ok(Arc::new(EmptyExec::new(false, schema)))
        } else {
            self.delta.scan(session, projection, filters, limit).await
        }
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        #[allow(deprecated)]
        self.delta.supports_filter_pushdown(filter)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.delta.statistics()
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self.insert_exec(input))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use object_store_util::conf::StorageConfig;
    use protogen::metastore::types::{
        catalog::{EntryMeta, EntryType, TableEntry},
        options::{InternalColumnDefinition, TableOptions, TableOptionsInternal},
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::native::access::NativeTableStorage;

    #[tokio::test]
    async fn test_delete_table() {
        let db_id = Uuid::new_v4();
        let dir = tempdir().unwrap();

        let storage = NativeTableStorage::from_config(
            db_id,
            StorageConfig::Local {
                path: dir.path().to_path_buf(),
            },
        )
        .unwrap();

        let entry = TableEntry {
            meta: EntryMeta {
                entry_type: EntryType::Table,
                id: 12345,
                parent: 54321,
                name: "table_1".to_string(),
                builtin: false,
                external: false,
                is_temp: false,
            },
            options: TableOptions::Internal(TableOptionsInternal {
                columns: vec![InternalColumnDefinition {
                    name: "id".to_string(),
                    nullable: true,
                    arrow_type: DataType::Int32,
                }],
            }),
            tunnel_id: None,
        };

        // Create a table, load it, delete it and load it again!
        storage.create_table(&entry).await.unwrap();
        storage.load_table(&entry).await.unwrap();
        storage.delete_table(&entry).await.unwrap();
        let err = storage
            .load_table(&entry)
            .await
            .map_err(|_| "Error loading table")
            .unwrap_err();
        assert_eq!(err, "Error loading table");
    }
}
