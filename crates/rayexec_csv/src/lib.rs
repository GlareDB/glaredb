pub mod datatable;
pub mod reader;

mod decoder;
mod read_csv;

use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::catalog::Catalog, datasource::DataSource, functions::table::GenericTableFunction,
    runtime::ExecutionRuntime,
};
use read_csv::ReadCsv;
use regex::{Regex, RegexBuilder};
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

    fn initialize_table_functions(&self) -> Vec<Box<dyn GenericTableFunction>> {
        vec![Box::new(ReadCsv)]
    }

    fn file_handlers(&self) -> Vec<(Regex, Box<dyn GenericTableFunction>)> {
        let file_regex = RegexBuilder::new(r"^.*\.(csv)$")
            .case_insensitive(true)
            .build()
            .expect("regex to build");
        vec![(file_regex, Box::new(ReadCsv))]
    }
}
