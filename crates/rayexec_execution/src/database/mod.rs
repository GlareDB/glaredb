pub mod builtin_views;
pub mod catalog;
pub mod catalog_entry;
pub mod create;
pub mod drop;
pub mod memory_catalog;
pub mod system;

mod catalog_map;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use catalog::CatalogTx;
use create::{CreateSchemaInfo, OnConflict};
use memory_catalog::MemoryCatalog;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_proto::ProtoConv;

use crate::storage::catalog_storage::CatalogStorage;
use crate::storage::memory::MemoryTableStorage;
use crate::storage::table_storage::TableStorage;

#[derive(Debug, Clone, PartialEq)]
pub struct AttachInfo {
    /// Name of the data source this attached database is for.
    pub datasource: String,
    /// Options used for connecting to the database.
    ///
    /// This includes things like connection strings, and other possibly
    /// sensitive info.
    pub options: HashMap<String, OwnedScalarValue>,
}

impl ProtoConv for AttachInfo {
    type ProtoType = rayexec_proto::generated::catalog::AttachInfo;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            datasource: self.datasource.clone(),
            options: self
                .options
                .iter()
                .map(|(k, v)| Ok((k.clone(), v.to_proto()?)))
                .collect::<Result<_>>()?,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            datasource: proto.datasource,
            options: proto
                .options
                .into_iter()
                .map(|(k, v)| Ok((k, ProtoConv::from_proto(v)?)))
                .collect::<Result<_>>()?,
        })
    }
}

/// An attached database.
#[derive(Debug, Clone)]
pub struct Database {
    /// In-memory catalog for the database.
    pub catalog: Arc<MemoryCatalog>,
    /// Storage for the catalog.
    ///
    /// If not provided, catalog changes won't be persisted (e.g. systema and
    /// temp catalogs).
    pub catalog_storage: Option<Arc<dyn CatalogStorage>>,
    /// Storage for tables.
    // TODO: Should probably never be None. Right now, only 'system' is None.
    pub table_storage: Option<Arc<dyn TableStorage>>,
    /// How we attached the catalog (for remote execution).
    pub attach_info: Option<AttachInfo>,
}

/// Root of all accessible catalogs.
///
/// Attaching external databases falls outside the normal catalog flow, and so
/// will not follow the same transactional semantics.
#[derive(Debug)]
pub struct DatabaseContext {
    databases: HashMap<String, Database>,
}

impl DatabaseContext {
    /// Creates a new database context containing containing a builtin "system"
    /// catalog, and a "temp" catalog for temporary database items.
    ///
    /// By itself, this context cannot be used to persist data. Additional
    /// catalogs need to be attached via `attach_catalog`.
    pub fn new(system_catalog: Arc<MemoryCatalog>) -> Result<Self> {
        // TODO: Make system catalog actually read-only.
        let mut databases = HashMap::new();

        databases.insert(
            "system".to_string(),
            Database {
                catalog: system_catalog,
                catalog_storage: None,
                table_storage: None,
                attach_info: None,
            },
        );

        let temp = MemoryCatalog::default();
        temp.create_schema(
            &CatalogTx {},
            &CreateSchemaInfo {
                name: "temp".to_string(),
                on_conflict: OnConflict::Error,
            },
        )?;

        databases.insert(
            "temp".to_string(),
            Database {
                catalog: Arc::new(temp),
                catalog_storage: None,
                table_storage: Some(Arc::new(MemoryTableStorage::default())),
                attach_info: None,
            },
        );

        Ok(DatabaseContext { databases })
    }

    pub fn system_catalog(&self) -> Result<&MemoryCatalog> {
        self.databases
            .get("system")
            .map(|d| d.catalog.as_ref())
            .ok_or_else(|| RayexecError::new("Missing system catalog"))
    }

    pub fn attach_database(&mut self, name: impl Into<String>, database: Database) -> Result<()> {
        let name = name.into();
        if self.databases.contains_key(&name) {
            return Err(RayexecError::new(format!(
                "Catalog with name '{name}' already attached"
            )));
        }
        self.databases.insert(name, database);

        Ok(())
    }

    pub fn detach_database(&mut self, name: &str) -> Result<()> {
        if self.databases.remove(name).is_none() {
            return Err(RayexecError::new(format!(
                "Database with name '{name}' doesn't exist"
            )));
        }
        Ok(())
    }

    pub fn database_exists(&self, name: &str) -> bool {
        self.databases.contains_key(name)
    }

    pub fn get_database(&self, name: &str) -> Result<&Database> {
        self.databases
            .get(name)
            .ok_or_else(|| RayexecError::new(format!("Missing catalog '{name}'")))
    }

    pub fn iter_databases(&self) -> impl Iterator<Item = (&String, &Database)> {
        self.databases.iter()
    }
}
