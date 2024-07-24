pub mod copy_to;
pub mod functions;
pub mod metadata;
pub mod reader;
pub mod writer;

mod schema;

use copy_to::ParquetCopyToFunction;
use functions::read_parquet::ReadParquet;
use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::catalog::Catalog,
    datasource::{DataSource, FileHandler},
    functions::table::TableFunction,
    runtime::ExecutionRuntime,
};
use regex::RegexBuilder;
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

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadParquet)]
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        let regex = RegexBuilder::new(r"^.*\.(parquet)$")
            .case_insensitive(true)
            .build()
            .expect("regex to build");

        vec![FileHandler {
            regex,
            table_func: Box::new(ReadParquet),
            copy_to: Some(Box::new(ParquetCopyToFunction)),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_regex() {
        let handlers = ParquetDataSource.file_handlers();
        let regex = &handlers[0].regex;

        assert!(regex.is_match("file.parquet"));
        assert!(regex.is_match("file.PARQUET"));
        assert!(regex.is_match("dir/*.parquet"));
        assert!(regex.is_match("dir/[0-10].parquet"));

        assert!(!regex.is_match("file.csv"));
        assert!(!regex.is_match("file.*"));
    }
}
