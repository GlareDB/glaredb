use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::memory::MemoryCatalog;
use crate::arrays::scalar::ScalarValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    ReadWrite,
    ReadOnly,
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
    pub(crate) mode: AccessMode,
    // TODO: Allow other catalog types.
    pub(crate) catalog: MemoryCatalog,
    pub(crate) attach_info: Option<AttachInfo>,
}

/// Accessible catalogs for a session.
#[derive(Debug)]
pub struct DatabaseContext {
    databases: HashMap<String, Arc<Database>>,
}

impl DatabaseContext {
    pub fn new(system_catalog: Arc<Database>) -> Result<Self> {
        unimplemented!()
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
