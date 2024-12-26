use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_error::{RayexecError, Result};
use regex::Regex;

use crate::arrays::scalar::OwnedScalarValue;
use crate::functions::copy::CopyToFunction;
use crate::functions::table::TableFunction;
use crate::runtime::Runtime;
use crate::storage::catalog_storage::CatalogStorage;
use crate::storage::memory::MemoryTableStorage;
use crate::storage::table_storage::TableStorage;

// TODO: COPY TO function registration:
//
// - Return functions with a FORMAT specifier to allow users to be explicit
//   instead of relying on implicit inferrence.
//
//   `COPY (SELECT ...) TO 'my_path' (FORMAT parquet)`

/// Trait for constructing data sources.
///
/// This mostly exists to describe how runtimes get injected into data sources,
/// and is not explicitly necessary.
pub trait DataSourceBuilder<R: Runtime>: Sync + Send + Debug + Sized {
    /// Initialize this data source using the provided runtime.
    ///
    /// This should create the data source object which gets inserted into
    /// registry. The runtime should be cloned and stored in the object if
    /// anything (table function, catalog) requires something that's provided by
    /// the runtime, like a tokio handle or http client.
    fn initialize(runtime: R) -> Box<dyn DataSource>;
}

#[derive(Debug)]
pub struct DataSourceConnection {
    pub catalog_storage: Option<Arc<dyn CatalogStorage>>,
    pub table_storage: Arc<dyn TableStorage>,
}

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
// TODO: Rename to DataConnector?
pub trait DataSource: Sync + Send + Debug {
    fn connect(
        &self,
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        Box::pin(async {
            Err(RayexecError::new(
                "Connect not implemented for this data source",
            ))
        })
    }

    /// Initialize a list of table functions that this data source provides.
    ///
    /// Note that these functions should be stateless, as they are registered
    /// into the system catalog at startup.
    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        Vec::new()
    }

    /// Initialize a list of COPY TO functions for this data source.
    ///
    /// Similar to table functions, these should be stateless, and get
    /// registered into the system catalog on startup.
    fn initialize_copy_to_functions(&self) -> Vec<DataSourceCopyTo> {
        Vec::new()
    }

    /// Return file handlers that this data souce can handle.
    ///
    /// During binding, these file handlers will be used to determine if there's
    /// a function that's able to handle a file path provided by the user in the
    /// FROM segment of the query. The returned pair contains a regex and
    /// function pair that indicate if that function is able to handle the file
    /// path.
    ///
    /// For example, if the user provides the following query:
    ///
    /// SELECT * FROM 'dir/*.parquet'
    ///
    /// All registered file handlers will be provided the string
    /// 'dir/*.parquet'. If the regex matches, then the function is inserted
    /// into the query, essentially being rewritten as:
    ///
    /// SELECT * FROM read_parquet('dir/*.parqet')
    ///
    /// It's assumed that the file path is always the first argument to the
    /// function.
    ///
    /// This is called once when registering a data source. Lazy is not needed.
    fn file_handlers(&self) -> Vec<FileHandler> {
        Vec::new()
    }
}

// TODO: This, the file handlers, and table functions returned from a data
// source can probably be cleaned up. Currently some duplication.
#[derive(Debug)]
pub struct DataSourceCopyTo {
    /// Format this COPY TO function is for.
    ///
    /// Corresponds to the FORMAT option in:
    ///
    /// COPY <source> TO <dest> (FORMAT <format>)
    pub format: String,

    /// The COPY TO function.
    pub copy_to: Box<dyn CopyToFunction>,
}

#[derive(Debug)]
pub struct FileHandler {
    /// Regex to use to determine if this handler should handle the file.
    pub regex: Regex,

    /// Table function to use to read the file.
    pub table_func: Box<dyn TableFunction>,

    /// Optional copy to function for writing out files.
    pub copy_to: Option<Box<dyn CopyToFunction>>,
}

#[derive(Debug, Default)]
pub struct FileHandlers {
    /// Registered file handlers for resolving file paths in FROM statements.
    handlers: Vec<FileHandler>,
}

impl FileHandlers {
    pub const fn empty() -> Self {
        FileHandlers {
            handlers: Vec::new(),
        }
    }

    pub fn find_match(&self, path: &str) -> Option<&FileHandler> {
        self.handlers
            .iter()
            .find(|handler| handler.regex.is_match(path))
    }
}

#[derive(Debug, Default)]
pub struct DataSourceRegistry {
    datasources: HashMap<String, Box<dyn DataSource>>,
    file_handlers: FileHandlers,
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

        self.file_handlers
            .handlers
            .extend(datasource.file_handlers());
        self.datasources.insert(name, datasource);

        Ok(self)
    }

    pub fn get_datasource(&self, name: &str) -> Option<&dyn DataSource> {
        self.datasources.get(name).map(|d| d.as_ref())
    }

    pub fn get_file_handlers(&self) -> &FileHandlers {
        &self.file_handlers
    }

    /// Iterate all data sources.
    pub fn iter(&self) -> impl Iterator<Item = &dyn DataSource> {
        self.datasources.values().map(|d| d.as_ref())
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
    fn connect(
        &self,
        options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        Box::pin(async move {
            if !options.is_empty() {
                return Err(RayexecError::new("Memory data source takes no options"));
            }

            Ok(DataSourceConnection {
                catalog_storage: None,
                table_storage: Arc::new(MemoryTableStorage::default()),
            })
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        Vec::new()
    }
}
