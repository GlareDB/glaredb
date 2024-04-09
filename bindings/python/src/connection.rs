use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan as DFLogicalPlan;
use datafusion_ext::vars::SessionVars;
use futures::lock::Mutex;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::PyType;
use sqlexec::engine::{Engine, EngineStorage, SessionStorageConfig, TrackedSession};
use sqlexec::{LogicalPlan, OperationInfo};

use crate::execution_result::PyExecutionResult;

pub(super) type PyTrackedSession = Arc<Mutex<TrackedSession>>;

use crate::error::PyGlareDbError;
use crate::logical_plan::PyLogicalPlan;
use crate::runtime::wait_for_future;

/// A connected session to a GlareDB database.
#[pyclass]
#[derive(Clone)]
pub struct Connection {
    pub(super) sess: PyTrackedSession,
    pub(super) _engine: Arc<Engine>,
}

impl Connection {
    /// Returns a default connection to an in-memory database.
    ///
    /// The database is only initialized once, and all subsequent calls will
    /// return the same connection.
    pub fn default_in_memory(py: Python<'_>) -> PyResult<Self> {
        static DEFAULT_CON: OnceCell<Connection> = OnceCell::new();

        let con = DEFAULT_CON.get_or_try_init(|| {
            wait_for_future(py, async move {
                let engine = Engine::from_storage(EngineStorage::Memory).await?;
                let sess = engine
                    .new_local_session_context(
                        SessionVars::default(),
                        SessionStorageConfig::default(),
                    )
                    .await?;
                Ok(Connection {
                    sess: Arc::new(Mutex::new(sess)),
                    _engine: Arc::new(engine),
                }) as Result<_, PyGlareDbError>
            })
        })?;

        Ok(con.clone())
    }
}

#[pymethods]
impl Connection {
    fn __enter__(&mut self, _py: Python<'_>) -> PyResult<Self> {
        Ok(self.clone())
    }

    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<&PyType>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<()> {
        self.close(py)?;
        Ok(())
    }

    /// Run a SQL operation against a GlareDB database.
    ///
    /// All operations that write or modify data are executed
    /// directly, but all query operations run lazily when you process
    /// their results with `show`, `to_arrow`, `to_pandas`, or
    /// `to_polars`, or call the `execute` method.
    ///
    /// # Examples
    ///
    /// Show the output of a query.
    ///
    /// ```python
    /// import glaredb
    ///
    /// con = glaredb.connect()
    /// con.sql('select 1').show()
    /// ```
    ///
    /// Convert the output of a query to a Pandas dataframe.
    ///
    /// ```python
    /// import glaredb
    /// import pandas
    ///
    /// con = glaredb.connect()
    /// my_df = con.sql('select 1').to_pandas()
    /// ```
    ///
    /// Execute the query to completion, returning no output. This is useful
    /// when the query output doesn't matter, for example, creating a table or
    /// inserting data into a table.
    ///
    /// ```python
    /// import glaredb
    /// import pandas
    ///
    /// con = glaredb.connect()
    /// con.sql('create table my_table (a int)').execute()
    /// ```
    pub fn sql(&mut self, py: Python<'_>, query: &str) -> PyResult<PyLogicalPlan> {
        let cloned_sess = self.sess.clone();
        wait_for_future(py, async move {
            let mut sess = self.sess.lock().await;

            let plan = sess
                .create_logical_plan(query)
                .await
                .map_err(PyGlareDbError::from)?;

            let op = OperationInfo::new().with_query_text(query);

            match plan
                .to_owned()
                .try_into_datafusion_plan()
                .expect("resolving logical plan")
            {
                DFLogicalPlan::Extension(_)
                | DFLogicalPlan::Dml(_)
                | DFLogicalPlan::Ddl(_)
                | DFLogicalPlan::Copy(_) => {
                    sess.execute_logical_plan(plan, &op)
                        .await
                        .map_err(PyGlareDbError::from)?;

                    Ok(PyLogicalPlan::new(
                        LogicalPlan::Noop,
                        cloned_sess,
                        Default::default(),
                    ))
                }
                _ => Ok(PyLogicalPlan::new(plan, cloned_sess, op)),
            }
        })
    }

    /// Run a PRQL query against a GlareDB database. Does not change
    /// the state or dialect of the connection object.
    ///
    /// ```python
    /// import glaredb
    /// import pandas
    ///
    /// con = glaredb.connect()
    /// my_df = con.prql('from my_table | take 1').to_pandas()
    /// ```
    ///
    /// All operations execute lazily when their results are
    /// processed.
    pub fn prql(&mut self, py: Python<'_>, query: &str) -> PyResult<PyLogicalPlan> {
        let cloned_sess = self.sess.clone();
        wait_for_future(py, async move {
            let mut sess = self.sess.lock().await;
            let plan = sess.prql_to_lp(query).await.map_err(PyGlareDbError::from)?;
            let op = OperationInfo::new().with_query_text(query);

            Ok(PyLogicalPlan::new(plan, cloned_sess, op))
        })
    }

    /// Execute a SQL query.
    ///
    /// # Examples
    ///
    /// Creating a table.
    ///
    /// ```python
    /// import glaredb
    ///
    /// con = glaredb.connect()
    /// con.execute('create table my_table (a int)')
    /// ```
    pub fn execute(&mut self, py: Python<'_>, query: &str) -> PyResult<PyExecutionResult> {
        let sess = self.sess.clone();
        let (_, exec_result) = wait_for_future(py, async move {
            let mut sess = sess.lock().await;
            let plan = sess
                .create_logical_plan(query)
                .await
                .map_err(PyGlareDbError::from)?;

            let op = OperationInfo::new().with_query_text(query);

            sess.execute_logical_plan(plan, &op)
                .await
                .map_err(PyGlareDbError::from)
        })?;

        Ok(PyExecutionResult(exec_result))
    }

    /// Close the current session.
    pub fn close(&mut self, _py: Python<'_>) -> PyResult<()> {
        // TODO: Remove this method. No longer required.
        Ok(())
    }
}
