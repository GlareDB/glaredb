use std::collections::HashMap;

use rayexec_error::{RayexecError, Result};

use super::create::CreateViewInfo;
use super::memory::{MemoryCatalog, MemoryCatalogTx};
use super::CatalogPlanner;
use crate::arrays::scalar::ScalarValue;
use crate::execution::operators::PlannedOperator;

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
    pub(crate) name: String,
    pub(crate) mode: AccessMode,
    // TODO: Allow other catalog types.
    //
    // This will be a common type for both 'Catalog' and 'CatalogPlanner'.
    pub(crate) catalog: MemoryCatalog,
    pub(crate) attach_info: Option<AttachInfo>,
}

impl Database {
    pub fn plan_create_view(
        &self,
        tx: &MemoryCatalogTx, // TODO
        schema: &str,
        create: CreateViewInfo,
    ) -> Result<PlannedOperator> {
        self.check_can_write()?;
        self.catalog.plan_create_view(&tx, schema, create)
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
