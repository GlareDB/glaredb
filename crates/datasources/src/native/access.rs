use crate::native::errors::{NativeError, Result};
use datafusion::datasource::TableProvider;
use deltalake::action::SaveMode;
use deltalake::operations::create::CreateBuilder;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaTable, DeltaTableConfig};
use metastoreproto::types::catalog::TableEntry;
use metastoreproto::types::options::{TableOptions, TableOptionsInternal};
use object_store::{path::Path as ObjectPath, prefix::PrefixStore};
use object_store_util::{conf::StorageConfig, shared::SharedObjectStore};
use std::sync::Arc;
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

    pub async fn create_table(&self, table: &TableEntry) -> Result<NativeTable> {
        let loc = self.storage_url_for_table(table)?;
        let store = self.create_delta_store(loc.clone())?;

        let opts = Self::opts_from_ent(table)?;
        let mut builder = CreateBuilder::new()
            .with_table_name(&table.meta.name)
            .with_location(loc)
            .with_object_store(store)
            .with_save_mode(SaveMode::ErrorIfExists);
        for col in &opts.columns {
            builder =
                builder.with_column(&col.name, (&col.arrow_type).try_into()?, col.nullable, None);
        }

        // TODO: Partitioning

        let table = builder.await?;

        Ok(NativeTable { delta: table })
    }

    /// Load a native table.
    ///
    /// Errors if the table is not the correct type.
    pub async fn load_table(&self, table: &TableEntry) -> Result<NativeTable> {
        let _ = Self::opts_from_ent(table)?; // Check that this is the correct table type.

        let loc = self.storage_url_for_table(table)?;
        let delta_store = self.create_delta_store(loc)?;
        let mut table = DeltaTable::new(
            delta_store,
            DeltaTableConfig {
                require_tombstones: true,
                require_files: true,
            },
        );

        table.load().await?;

        Ok(NativeTable { delta: table })
    }

    fn create_delta_store(&self, loc: Url) -> Result<Arc<DeltaObjectStore>> {
        let prefix = ObjectPath::parse(loc.path())?;
        let prefixed = PrefixStore::new(self.store.clone(), prefix);
        let delta_store = DeltaObjectStore::new(Arc::new(prefixed), loc);
        Ok(Arc::new(delta_store))
    }

    fn opts_from_ent(table: &TableEntry) -> Result<&TableOptionsInternal> {
        let opts = match &table.options {
            TableOptions::Internal(opts) => opts,
            _ => return Err(NativeError::NotNative(table.clone())),
        };
        Ok(opts)
    }

    /// Get a url pointing to the physical location of the table.
    fn storage_url_for_table(&self, table: &TableEntry) -> Result<Url> {
        let url = match &self.conf {
            StorageConfig::Gcs { bucket, .. } => {
                let s = format!(
                    "gs://{bucket}/databases/{}/tables/{}",
                    self.db_id, table.meta.id
                );
                Url::parse(&s)?
            }
            StorageConfig::Local { path } => {
                let path =
                    std::fs::canonicalize(path).map_err(|e| NativeError::CanonicalizePath {
                        path: path.clone(),
                        e,
                    })?;
                let path = path.join(format!("tables/{}", table.meta.id));
                Url::from_file_path(path).map_err(|_| NativeError::Static("Path not absolute"))?
            }
            StorageConfig::Memory => {
                let s = format!("memory://tables/{}", table.meta.id);
                Url::parse(&s)?
            }
        };

        Ok(url)
    }
}

pub struct NativeTable {
    delta: DeltaTable,
}

impl NativeTable {
    pub fn storage_location(&self) -> String {
        self.delta.table_uri()
    }

    pub fn into_table_provider(self) -> Arc<dyn TableProvider> {
        Arc::new(self.delta)
    }
}
