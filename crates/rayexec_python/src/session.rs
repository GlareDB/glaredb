use pyo3::{pyclass, pyfunction, pymethods, Python};

use crate::{errors::Result, event_loop::run_until_complete, table::PythonTable};

use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource};
use rayexec_parquet::ParquetDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::{ResultTable, SingleUserEngine};

#[pyfunction]
pub fn connect() -> Result<PythonSession> {
    // TODO: Pass in a tokio runtime.
    let runtime = NativeRuntime::with_default_tokio()?;
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("parquet", ParquetDataSource::initialize(runtime.clone()))?
        .with_datasource("csv", CsvDataSource::initialize(runtime.clone()))?
        .with_datasource("delta", DeltaDataSource::initialize(runtime.clone()))?;

    let executor = ThreadedNativeExecutor::try_new()?;
    let engine = SingleUserEngine::try_new(executor, runtime.clone(), registry)?;

    Ok(PythonSession { engine })
}

#[pyclass]
#[derive(Debug)]
pub struct PythonSession {
    pub(crate) engine: SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>,
}

#[pymethods]
impl PythonSession {
    /// Runs a single query, returning the results.
    fn query(&mut self, py: Python, sql: String) -> Result<PythonTable> {
        let session = self.engine.session().clone();
        let table = run_until_complete(py, async move {
            let result = session.query(&sql).await?;
            let table = PythonTable {
                table: ResultTable::collect_from_result_stream(result).await?,
            };

            Ok(table)
        })?;

        Ok(table)
    }
}
