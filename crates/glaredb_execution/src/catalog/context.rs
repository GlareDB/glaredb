use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use glaredb_error::{DbError, Result};

use super::database::{AccessMode, Database};
use super::memory::MemoryCatalog;
use super::profile::ProfileCollector;
use crate::catalog::Catalog;
use crate::catalog::create::{CreateSchemaInfo, OnConflict};
use crate::storage::storage_manager::StorageManager;

pub const SYSTEM_CATALOG: &str = "system";
pub const TEMP_CATALOG: &str = "temp";

/// Accessible catalogs for a session.
#[derive(Debug)]
pub struct DatabaseContext {
    databases: HashMap<String, Arc<Database>>,
    profiles: Arc<ProfileCollector>,
}

impl DatabaseContext {
    pub fn new(system_catalog: Arc<Database>) -> Result<Self> {
        let mut databases = HashMap::new();
        databases.insert(system_catalog.name.clone(), system_catalog);

        let temp_db = Arc::new(Database {
            name: TEMP_CATALOG.to_string(),
            mode: AccessMode::ReadWrite,
            catalog: Arc::new(MemoryCatalog::empty()),
            storage: Arc::new(StorageManager::empty()),
            attach_info: None,
        });

        temp_db.catalog.create_schema(&CreateSchemaInfo {
            name: "temp".to_string(),
            on_conflict: OnConflict::Error,
        })?;

        databases.insert(temp_db.name.clone(), temp_db);

        Ok(DatabaseContext {
            databases,
            profiles: Arc::new(ProfileCollector::default()),
        })
    }

    pub fn profiles(&self) -> &Arc<ProfileCollector> {
        &self.profiles
    }

    pub fn get_database(&self, name: &str) -> Option<&Arc<Database>> {
        self.databases.get(name)
    }

    pub fn require_get_database(&self, name: &str) -> Result<&Arc<Database>> {
        self.databases
            .get(name)
            .ok_or_else(|| DbError::new(format!("Missing catalog '{name}'")))
    }

    pub fn iter_databases(&self) -> impl Iterator<Item = &Arc<Database>> + '_ {
        self.databases.values()
    }
}
