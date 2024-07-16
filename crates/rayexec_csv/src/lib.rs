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
    datasource::{DataSource, FileHandler},
    functions::table::TableFunction,
    runtime::ExecutionRuntime,
};
use read_csv::ReadCsv;
use regex::RegexBuilder;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvDataSource;

impl DataSource for CsvDataSource {
    fn create_catalog(
        &self,
        _runtime: &Arc<dyn ExecutionRuntime>,
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(async {
            Err(RayexecError::new(
                "CSV data source cannot be used to create a catalog",
            ))
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadCsv)]
    }

    fn file_handlers(&self) -> Vec<FileHandler> {
        let regex = RegexBuilder::new(r"^.*\.(csv)$")
            .case_insensitive(true)
            .build()
            .expect("regex to build");

        vec![FileHandler {
            regex,
            table_func: Box::new(ReadCsv),
            copy_to: Some(Box::new(CsvCopyToFunction)),
        }]
    }
}
