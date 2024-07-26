pub mod copy_to;
pub mod datatable;
pub mod reader;
pub mod writer;

mod decoder;
mod read_csv;

use copy_to::CsvCopyToFunction;
use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::catalog::Catalog,
    datasource::{DataSource, DataSourceBuilder, FileHandler},
    functions::table::TableFunction,
    runtime::Runtime,
};
use read_csv::ReadCsv;
use regex::RegexBuilder;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvDataSource<R: Runtime> {
    runtime: R,
}

impl<R: Runtime> DataSourceBuilder<R> for CsvDataSource<R> {
    fn initialize(runtime: R) -> Box<dyn DataSource> {
        Box::new(CsvDataSource { runtime })
    }
}

impl<R: Runtime> DataSource for CsvDataSource<R> {
    fn create_catalog(
        &self,
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(async {
            Err(RayexecError::new(
                "CSV data source cannot be used to create a catalog",
            ))
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadCsv {
            runtime: self.runtime.clone(),
        })]
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        let regex = RegexBuilder::new(r"^.*\.(csv)$")
            .case_insensitive(true)
            .build()
            .expect("regex to build");

        vec![FileHandler {
            regex,
            table_func: Box::new(ReadCsv {
                runtime: self.runtime.clone(),
            }),
            copy_to: Some(Box::new(CsvCopyToFunction {
                runtime: self.runtime.clone(),
            })),
        }]
    }
}
