use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::database::catalog::Catalog;
use crate::database::storage::memory::MemoryCatalog;
use crate::engine::EngineRuntime;

pub trait DataSource: Sync + Send + Debug {
    /// Create a new catalog using the provided options.
    fn create_catalog(
        &self,
        runtime: &Arc<EngineRuntime>,
        options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>>;
}

#[derive(Debug, Default)]
pub struct DataSourceRegistry {
    datasources: HashMap<String, Box<dyn DataSource>>,
}

impl DataSourceRegistry {
    pub fn with_datasource(
        mut self,
        name: impl Into<String>,
        datasource: Box<dyn DataSource>,
    ) -> Result<Self> {
        let name = name.into();
        if self.datasources.contains_key(&name) {
            return Err(RayexecError::new(format!(
                "Duplicate data source with name '{name}'"
            )));
        }
        self.datasources.insert(name, datasource);
        Ok(self)
    }

    pub fn get_datasource(&self, name: &str) -> Result<&dyn DataSource> {
        self.datasources
            .get(name)
            .map(|d| d.as_ref())
            .ok_or_else(|| RayexecError::new(format!("Missing data source: {name}")))
    }
}

/// Take an option from the options map, returning an error if it doesn't exist.
pub fn take_option(
    name: &str,
    options: &mut HashMap<String, OwnedScalarValue>,
) -> Result<OwnedScalarValue> {
    options
        .remove(name)
        .ok_or_else(|| RayexecError::new(format!("Missing required option '{name}'")))
}

/// Check that options is empty, erroring if it isn't.
pub fn check_options_empty(options: &HashMap<String, OwnedScalarValue>) -> Result<()> {
    if options.is_empty() {
        return Ok(());
    }
    let extras = options
        .iter()
        .map(|(k, _)| format!("'{k}'"))
        .collect::<Vec<_>>()
        .join(", ");

    Err(RayexecError::new(format!(
        "Unexpected extra arguments: {extras}"
    )))
}

#[derive(Debug)]
pub struct MemoryDataSource;

impl DataSource for MemoryDataSource {
    fn create_catalog(
        &self,
        _runtime: &Arc<EngineRuntime>,
        options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(async move {
            if !options.is_empty() {
                return Err(RayexecError::new("Memory data source takes no options"));
            }

            Ok(Box::new(MemoryCatalog::new_with_schema("public")) as _)
        })
    }
}
