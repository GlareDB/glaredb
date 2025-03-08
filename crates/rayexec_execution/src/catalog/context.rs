use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::database::{AccessMode, Database};
use super::memory::MemoryCatalog;
use crate::catalog::create::{CreateSchemaInfo, OnConflict};
use crate::catalog::memory::MemoryCatalogTx;
use crate::catalog::Catalog;

pub const SYSTEM_CATALOG: &str = "system";
pub const TEMP_CATALOG: &str = "temp";

/// Accessible catalogs for a session.
#[derive(Debug)]
pub struct DatabaseContext {
    databases: HashMap<String, Arc<Database>>,
}

impl DatabaseContext {
    pub fn new(system_catalog: Arc<Database>) -> Result<Self> {
        let mut databases = HashMap::new();
        databases.insert(system_catalog.name.clone(), system_catalog);

        let temp_db = Arc::new(Database {
            name: TEMP_CATALOG.to_string(),
            mode: AccessMode::ReadWrite,
            catalog: MemoryCatalog::empty(),
            attach_info: None,
        });

        temp_db.catalog.create_schema(
            &MemoryCatalogTx {},
            &CreateSchemaInfo {
                name: "temp".to_string(),
                on_conflict: OnConflict::Error,
            },
        )?;

        databases.insert(temp_db.name.clone(), temp_db);

        Ok(DatabaseContext { databases })
    }

    pub fn get_database(&self, name: &str) -> Option<&Arc<Database>> {
        self.databases.get(name)
    }

    pub fn require_get_database(&self, name: &str) -> Result<&Arc<Database>> {
        self.databases
            .get(name)
            .ok_or_else(|| RayexecError::new(format!("Missing catalog '{name}'")))
    }
}
