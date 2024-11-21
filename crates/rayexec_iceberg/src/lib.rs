pub mod spec;

mod read_iceberg;

use rayexec_execution::datasource::{DataSource, DataSourceBuilder, FileHandler};
use rayexec_execution::functions::table::TableFunction;
use rayexec_execution::runtime::Runtime;

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
    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        Vec::new()
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        Vec::new()
    }
}
