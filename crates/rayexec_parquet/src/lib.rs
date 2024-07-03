pub mod array;
pub mod functions;

mod metadata;
mod schema;

use functions::read_parquet::ReadParquet;
use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::catalog::Catalog, datasource::DataSource, functions::table::GenericTableFunction,
    runtime::ExecutionRuntime,
};
use regex::{Regex, RegexBuilder};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetDataSource;

impl DataSource for ParquetDataSource {
    fn create_catalog(
        &self,
        _runtime: &Arc<dyn ExecutionRuntime>,
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(async {
            Err(RayexecError::new(
                "Parquet data source cannot be used to create a catalog",
            ))
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn GenericTableFunction>> {
        vec![Box::new(ReadParquet)]
    }

    fn file_handlers(&self) -> Vec<(Regex, Box<dyn GenericTableFunction>)> {
        let file_regex = RegexBuilder::new(r"^.*\.(parquet)$")
            .case_insensitive(true)
            .build()
            .expect("regex to build");
        vec![(file_regex, Box::new(ReadParquet))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_regex() {
        let handlers = ParquetDataSource.file_handlers();
        let regex = &handlers[0].0;

        assert!(regex.is_match("file.parquet"));
        assert!(regex.is_match("file.PARQUET"));
        assert!(regex.is_match("dir/*.parquet"));
        assert!(regex.is_match("dir/[0-10].parquet"));

        assert!(!regex.is_match("file.csv"));
        assert!(!regex.is_match("file.*"));
    }
}
