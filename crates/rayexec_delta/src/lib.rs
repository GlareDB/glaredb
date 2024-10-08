pub mod protocol;

mod datatable;
mod read_delta;

use rayexec_execution::datasource::{DataSource, DataSourceBuilder, FileHandler};
use rayexec_execution::functions::table::TableFunction;
use rayexec_execution::runtime::Runtime;
use read_delta::ReadDelta;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaDataSource<R: Runtime> {
    runtime: R,
}

impl<R: Runtime> DataSourceBuilder<R> for DeltaDataSource<R> {
    fn initialize(runtime: R) -> Box<dyn DataSource> {
        Box::new(Self { runtime })
    }
}

impl<R: Runtime> DataSource for DeltaDataSource<R> {
    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadDelta {
            runtime: self.runtime.clone(),
        })]
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        Vec::new()
    }
}
