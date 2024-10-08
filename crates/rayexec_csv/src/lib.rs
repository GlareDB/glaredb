pub mod copy_to;
pub mod datatable;
pub mod reader;
pub mod writer;

mod decoder;
mod read_csv;

use copy_to::CsvCopyToFunction;
use rayexec_execution::datasource::{DataSource, DataSourceBuilder, DataSourceCopyTo, FileHandler};
use rayexec_execution::functions::table::TableFunction;
use rayexec_execution::runtime::Runtime;
use read_csv::ReadCsv;
use regex::RegexBuilder;

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
    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadCsv {
            runtime: self.runtime.clone(),
        })]
    }

    fn initialize_copy_to_functions(&self) -> Vec<DataSourceCopyTo> {
        vec![DataSourceCopyTo {
            format: "csv".to_string(),
            copy_to: Box::new(CsvCopyToFunction {
                runtime: self.runtime.clone(),
            }),
        }]
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
