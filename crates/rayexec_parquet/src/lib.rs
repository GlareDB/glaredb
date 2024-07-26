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
    datasource::{DataSource, DataSourceBuilder, FileHandler},
    functions::table::TableFunction,
    runtime::Runtime,
};
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetDataSource<R> {
    runtime: R,
}

impl<R: Runtime> DataSourceBuilder<R> for ParquetDataSource<R> {
    fn initialize(runtime: R) -> Box<dyn DataSource> {
        Box::new(Self { runtime })
    }
}

impl<R> ParquetDataSource<R> {
    fn file_regex() -> Regex {
        RegexBuilder::new(r"^.*\.(parquet)$")
            .case_insensitive(true)
            .build()
            .expect("regex to build")
    }
}

impl<R: Runtime> DataSource for ParquetDataSource<R> {
    fn create_catalog(
        &self,
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(async {
            Err(RayexecError::new(
                "Parquet data source cannot be used to create a catalog",
            ))
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadParquet {
            runtime: self.runtime.clone(),
        })]
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        vec![FileHandler {
            regex: Self::file_regex(),
            table_func: Box::new(ReadParquet {
                runtime: self.runtime.clone(),
            }),
            copy_to: Some(Box::new(ParquetCopyToFunction {
                runtime: self.runtime.clone(),
            })),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_regex() {
        let regex = ParquetDataSource::<()>::file_regex();

        assert!(regex.is_match("file.parquet"));
        assert!(regex.is_match("file.PARQUET"));
        assert!(regex.is_match("dir/*.parquet"));
        assert!(regex.is_match("dir/[0-10].parquet"));

        assert!(!regex.is_match("file.csv"));
        assert!(!regex.is_match("file.*"));
    }
}
