use pyo3::{pyclass, pyfunction, pymethods, Python};
use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_error::RayexecError;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource};
use rayexec_execution::engine::single_user::SingleUserEngine;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};

use crate::errors::Result;
use crate::event_loop::run_until_complete;
use crate::table::PythonMaterializedResultTable;

#[pyfunction]
pub fn connect() -> Result<PythonSession> {
    // TODO: Pass in a tokio runtime.
    let runtime = NativeRuntime::with_default_tokio()?;
    let executor = ThreadedNativeExecutor::try_new()?;
    let engine = SingleUserEngine::try_new(executor, runtime.clone())?;

    Ok(PythonSession {
        engine: Some(engine),
    })
}

#[pyclass]
#[derive(Debug)]
pub struct PythonSession {
    /// Single user engine backing this session.
    ///
    /// Wrapped in an option so that we can properly drop it on close and error
    /// if the user tries to reuse the session.
    pub(crate) engine: Option<SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>>,
}

#[pymethods]
impl PythonSession {
    /// Runs a single query, returning the results.
    // TODO: Make the profile thing a kw.
    #[pyo3(signature = (sql, collect_profile_data=false, /))]
    fn query(
        &mut self,
        py: Python,
        sql: String,
        collect_profile_data: bool,
    ) -> Result<PythonMaterializedResultTable> {
        let session = self.try_get_engine()?.session().clone();
        let table = run_until_complete(py, async move {
            let table = session.query(&sql).await?;
            let table = if collect_profile_data {
                table.collect_with_execution_profile().await?
            } else {
                table.collect().await?
            };

            Ok(PythonMaterializedResultTable { table })
        })?;

        Ok(table)
    }

    fn close(&mut self, _py: Python) -> Result<()> {
        match self.engine.take() {
            Some(_) => {
                // Dropping it...
                //
                // Possibly do some network calls if needed.
                Ok(())
            }
            None => Err(RayexecError::new("Tried to close an already closed session").into()),
        }
    }
}

impl PythonSession {
    fn try_get_engine(&self) -> Result<&SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>> {
        let engine = self.engine.as_ref().ok_or_else(|| {
            RayexecError::new("Attempted to reuse session after it's already been closed")
        })?;
        Ok(engine)
    }
}
