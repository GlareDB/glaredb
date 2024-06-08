use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::database::catalog::Catalog;
use crate::database::storage::memory::MemoryCatalog;
use crate::engine::EngineRuntime;

/// An implementation of `DataSource` describes a data source type that we can
/// read from.
///
/// Implementations themselves do not contain state, and instead contain the
/// logic for creating data source specific catalogs from a set of options. It's
/// these catalogs that contain state (list of tables, external connections,
/// etc). Catalogs are always scoped to single session.
///
/// Rules for writing a data source:
///
/// - Do not use tokio's fs module. Use std::fs. Tokio internally uses std::fs,
///   and just sends it to a blocking thread. We don't want that, as it limits
///   our control on how things get read (e.g. we might want to have our own
///   blocking io threads).
/// - Prefer to the ComputeScheduler for async tasks that do heavy computation
///   (e.g. decompressing a zstd stream).
/// - Use the tokio runtime if required, but keep the scope small. For example,
///   the postgres data source will initialize the connection on the tokio
///   runtime, but them move the actual streaming of data to the
///   ComputeScheduler.
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
