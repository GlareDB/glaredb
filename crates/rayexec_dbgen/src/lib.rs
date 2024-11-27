use rayexec_execution::datasource::{DataSource, DataSourceBuilder};
use rayexec_execution::functions::table::TableFunction;
use rayexec_execution::runtime::Runtime;
use tpch::TpchGen;

pub mod tpch;

extern "C" {
    fn foo_function();
}

pub fn add(left: u64, right: u64) -> u64 {
    unsafe { foo_function() };
    left + right
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbgenDataSource<R> {
    runtime: R,
}

impl<R: Runtime> DataSourceBuilder<R> for DbgenDataSource<R> {
    fn initialize(runtime: R) -> Box<dyn DataSource> {
        Box::new(Self { runtime })
    }
}

impl<R: Runtime> DataSource for DbgenDataSource<R> {
    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(TpchGen {
            runtime: self.runtime.clone(),
        })]
    }
}
