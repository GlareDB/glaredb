use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::create::{CreateSchemaInfo, CreateViewInfo};
use super::memory::MemoryCatalog;
use super::CatalogPlanner;
use crate::arrays::scalar::ScalarValue;
use crate::execution::operators::PlannedOperator;

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
    /// Name of the data source this attached database is for.
    pub datasource: String,
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
    //
    // This will be a common type for both 'Catalog' and 'CatalogPlanner'.
    pub(crate) catalog: Arc<MemoryCatalog>,
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

    pub fn plan_create_schema(&self, create: CreateSchemaInfo) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog.plan_create_schema(create)
    }

    fn check_can_write(&self) -> Result<()> {
        if self.mode != AccessMode::ReadWrite {
            return Err(RayexecError::new(format!(
                "Database '{}' is not writable",
                self.name
            )));
        }
        Ok(())
    }
}
