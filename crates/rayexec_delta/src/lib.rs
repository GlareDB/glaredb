pub mod protocol;

mod datatable;
mod read_delta;

use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::catalog::Catalog,
    datasource::{DataSource, FileHandler},
    functions::table::TableFunction,
    runtime::ExecutionRuntime,
};
use read_delta::ReadDelta;
use std::{collections::HashMap, sync::Arc};

// TODO: How do we want to handle catalogs that provide data sources? Do we want
// to have "Glue" and "Unity" data sources that are separate from the base
// "Delta" data source?
//
// Current thoughts:
//
// - Base delta data source that registers `read_delta`. Cannot creata "delta"
//   catalog.
// - Secondary data sources for unity and glue. Will not register a `read_delta`
//   function, but may register other functions like `read_unity` etc.
//
// Having separate data source for the actual catalogs means we can keep the
// file format and catalog implementation separate. For example, glue could also
// have iceberg, etc tables in it, and having glue as a separate data source
// means we can dispatch to the appropriate table implementation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaDataSource;

impl DataSource for DeltaDataSource {
    fn create_catalog(
        &self,
        _runtime: &Arc<dyn ExecutionRuntime>,
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(async {
            Err(RayexecError::new(
                "Delta data source cannot be used to create a catalog",
            ))
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadDelta)]
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        Vec::new()
    }
}
