pub mod spec;

mod datatable;
mod read_iceberg;
mod table;

use rayexec_execution::datasource::{DataSource, DataSourceBuilder, FileHandler};
use rayexec_execution::functions::table::TableFunction2;
use rayexec_execution::runtime::Runtime;
use read_iceberg::ReadIceberg;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergDataSource<R: Runtime> {
    runtime: R,
}

impl<R: Runtime> DataSourceBuilder<R> for IcebergDataSource<R> {
    fn initialize(runtime: R) -> Box<dyn DataSource> {
        Box::new(Self { runtime })
    }
}

impl<R: Runtime> DataSource for IcebergDataSource<R> {
    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction2>> {
        vec![Box::new(ReadIceberg {
            runtime: self.runtime.clone(),
        })]
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        Vec::new()
    }
}
