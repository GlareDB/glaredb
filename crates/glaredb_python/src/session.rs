use ext_csv::extension::CsvExtension;
use ext_parquet::extension::ParquetExtension;
use ext_spark::SparkExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::DbError;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime, ThreadedNativeExecutor, new_tokio_runtime_for_io,
};
use pyo3::{Python, pyclass, pyfunction, pymethods};

use crate::errors::Result;
use crate::event_loop::run_until_complete;
use crate::print::pyprint;

const DEFAULT_TABLE_WIDTH: usize = 100;

#[pyfunction]
pub fn connect() -> Result<PythonSession> {
    let tokio_rt = new_tokio_runtime_for_io()?;
    let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());
    let executor = ThreadedNativeExecutor::try_new()?;

    let engine = SingleUserEngine::try_new(executor, runtime.clone())?;
    // TODO: We should ensure that the extensions we're registering are
    // consistent across the CLI, wasm, and here.
    engine.register_extension(SparkExtension)?;
    engine.register_extension(TpchGenExtension)?;
    engine.register_extension(CsvExtension)?;
    engine.register_extension(ParquetExtension)?;

    Ok(PythonSession {
        tokio_rt,
        engine: Some(engine),
    })
}

#[pyclass]
#[derive(Debug)]
pub struct PythonSession {
    /// Keep the tokio runtime on the session to ensure it doesn't get dropped.
    ///
    /// Our native runtime only holds onto a handle. By storing the runtime
    /// here, we ensure that the handle remains valid through the lifetime of
    /// the session.
    #[expect(unused)]
    pub(crate) tokio_rt: tokio::runtime::Runtime,
    /// Single user engine backing this session.
    ///
    /// Wrapped in an option so that we can properly drop it on close and error
    /// if the user tries to reuse the session.
    pub(crate) engine: Option<SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>>,
}

#[pymethods]
impl PythonSession {
    /// Runs a single query, returning the results.
    fn sql(&mut self, py: Python, sql: String) -> Result<PythonQueryResult> {
        self.query(py, sql)
    }

    fn query(&mut self, py: Python, sql: String) -> Result<PythonQueryResult> {
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
            None => Err(DbError::new("Tried to close an already closed session").into()),
        }
    }
}

impl PythonSession {
    fn try_get_engine(
        &self,
    ) -> Result<&SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>> {
        let engine = self.engine.as_ref().ok_or_else(|| {
            DbError::new("Attempted to reuse session after it's already been closed")
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
        let pretty = PrettyTable::try_new(
            &self.schema,
            &self.batches,
            DEFAULT_TABLE_WIDTH,
            None,
            PRETTY_COMPONENTS,
        )?;
        Ok(format!("{pretty}"))
    }

    fn show(&self, py: Python) -> Result<()> {
        let pretty = PrettyTable::try_new(
            &self.schema,
            &self.batches,
            DEFAULT_TABLE_WIDTH,
            None,
            PRETTY_COMPONENTS,
        )?;
        pyprint(pretty, py)
    }
}
