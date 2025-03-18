use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use glaredb_error::{DbError, Result};

use super::create::{CreateSchemaInfo, CreateTableInfo, CreateViewInfo};
use super::drop::DropInfo;
use super::entry::CatalogEntry;
use super::memory::MemoryCatalog;
use super::Catalog;
use crate::arrays::scalar::ScalarValue;
use crate::execution::operators::PlannedOperator;
use crate::storage::storage_manager::StorageManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    ReadWrite,
    ReadOnly,
}

impl AccessMode {
    pub fn as_str(&self) -> &str {
        match self {
            AccessMode::ReadWrite => "ReadWrite",
            AccessMode::ReadOnly => "ReadOnly",
        }
    }
}

impl fmt::Display for AccessMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachInfo {
    /// Options used for connecting to the database.
    ///
    /// This includes things like connection strings, and other possibly
    /// sensitive info.
    pub options: HashMap<String, ScalarValue>,
}

#[derive(Debug)]
pub struct Database {
    pub(crate) name: String,
    pub(crate) mode: AccessMode,
    // TODO: Allow other catalog types.
    pub(crate) catalog: Arc<MemoryCatalog>,
    // TODO: Allow other storage managers.
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) attach_info: Option<AttachInfo>,
}

impl Database {
    pub fn plan_create_view(
        &self,
        schema: &str,
        create: CreateViewInfo,
    ) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog.plan_create_view(schema, create)
    }

    pub fn plan_create_table(
        &self,
        schema: &str,
        create: CreateTableInfo,
    ) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog
            .plan_create_table(&self.storage, schema, create)
    }

    pub fn plan_create_table_as(
        &self,
        schema: &str,
        create: CreateTableInfo,
    ) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog
            .plan_create_table_as(&self.storage, schema, create)
    }

    pub fn plan_insert(&self, table: Arc<CatalogEntry>) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog.plan_insert(&self.storage, table)
    }

    pub fn plan_create_schema(&self, create: CreateSchemaInfo) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog.plan_create_schema(create)
    }

    pub fn plan_drop(&self, drop: DropInfo) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog.plan_drop(&self.storage, drop)
    }

    fn check_can_write(&self) -> Result<()> {
        if self.mode != AccessMode::ReadWrite {
            return Err(DbError::new(format!(
                "Database '{}' is not writable",
                self.name
            )));
        }
        Ok(())
    }
}
