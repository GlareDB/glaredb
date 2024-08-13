pub mod catalog;
pub mod create;
pub mod ddl;
pub mod drop;
pub mod entry;
pub mod storage;
pub mod table;

use catalog::Catalog;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use storage::memory::MemoryCatalog;
use storage::system::SystemCatalog;

/// Root of all accessible catalogs.
#[derive(Debug)]
pub struct DatabaseContext {
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl DatabaseContext {
    /// Creates a new database context containing containing a builtin "system"
    /// catalog, and a "temp" catalog for temporary database items.
    ///
    /// By itself, this context cannot be used to persist data. Additional
    /// catalogs need to be attached via `attach_catalog`.
    pub fn new(system_catalog: Arc<SystemCatalog>) -> Self {
        let catalogs = [
            ("system".to_string(), system_catalog as _),
            (
                "temp".to_string(),
                Arc::new(MemoryCatalog::new_with_schema("temp")) as _,
            ),
        ]
        .into_iter()
        .collect();

        DatabaseContext { catalogs }
    }

    pub fn system_catalog(&self) -> Result<&dyn Catalog> {
        self.catalogs
            .get("system")
            .map(|c| c.as_ref())
            .ok_or_else(|| RayexecError::new("Missing system catalog"))
    }

    pub fn attach_catalog(
        &mut self,
        name: impl Into<String>,
        catalog: Arc<dyn Catalog>,
    ) -> Result<()> {
        let name = name.into();
        if self.catalogs.contains_key(&name) {
            return Err(RayexecError::new(format!(
                "Catalog with name '{name}' already attached"
            )));
        }
        self.catalogs.insert(name, catalog);

        Ok(())
    }

    pub fn detach_catalog(&mut self, name: &str) -> Result<()> {
        if self.catalogs.remove(name).is_none() {
            return Err(RayexecError::new(format!(
                "Catalog with name '{name}' doesn't exist"
            )));
        }
        Ok(())
    }

    pub fn catalog_exists(&self, name: &str) -> bool {
        self.catalogs.contains_key(name)
    }

    pub fn get_catalog(&self, name: &str) -> Result<&dyn Catalog> {
        self.catalogs
            .get(name)
            .map(|c| c.as_ref())
            .ok_or_else(|| RayexecError::new(format!("Missing catalog '{name}'")))
    }

    pub(crate) fn iter_catalogs(&self) -> impl Iterator<Item = (&String, &Arc<dyn Catalog>)> {
        self.catalogs.iter()
    }
}
