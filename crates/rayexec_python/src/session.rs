use glaredb_error::RayexecError;
use glaredb_execution::arrays::batch::Batch;
use glaredb_execution::arrays::field::ColumnSchema;
use glaredb_execution::arrays::format::pretty::table::PrettyTable;
use glaredb_execution::engine::single_user::SingleUserEngine;
use glaredb_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use pyo3::{Python, pyclass, pyfunction, pymethods};

use crate::errors::Result;
use crate::event_loop::run_until_complete;
use crate::print::pyprint;

const DEFAULT_TABLE_WIDTH: usize = 100;

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
    ) -> Result<PythonQueryResult> {
        let _ = collect_profile_data; // TODO

        let session = self.try_get_engine()?.session().clone();
        let table = run_until_complete(py, async move {
            let mut q_res = session.query(&sql).await?;
            let batches = q_res.output.collect().await?;

            Ok(PythonQueryResult {
                schema: q_res.output_schema,
                batches,
            })
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

#[pyclass]
#[derive(Debug)]
pub struct PythonQueryResult {
    pub(crate) schema: ColumnSchema,
    pub(crate) batches: Vec<Batch>,
}

#[pymethods]
impl PythonQueryResult {
    fn __repr__(&self) -> Result<String> {
        let pretty = PrettyTable::try_new(&self.schema, &self.batches, DEFAULT_TABLE_WIDTH, None)?;
        Ok(format!("{pretty}"))
    }

    fn show(&self, py: Python) -> Result<()> {
        let pretty = PrettyTable::try_new(&self.schema, &self.batches, DEFAULT_TABLE_WIDTH, None)?;
        pyprint(pretty, py)
    }
}
