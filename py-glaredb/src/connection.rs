use crate::execution_result::PyExecutionResult;
use datafusion::logical_expr::LogicalPlan as DFLogicalPlan;
use datafusion_ext::vars::SessionVars;
use futures::lock::Mutex;
use once_cell::sync::OnceCell;
use pyo3::{prelude::*, types::PyType};
use sqlexec::engine::{Engine, SessionStorageConfig, TrackedSession};
use sqlexec::LogicalPlan;
use std::sync::Arc;

pub(super) type PyTrackedSession = Arc<Mutex<TrackedSession>>;

use crate::{error::PyGlareDbError, logical_plan::PyLogicalPlan, runtime::wait_for_future};

/// A connected session to a GlareDB database.
#[pyclass]
#[derive(Clone)]
pub struct Connection {
    pub(super) sess: PyTrackedSession,
    pub(super) engine: Arc<Engine>,
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
                let engine = Engine::from_data_dir(None).await?;
                let sess = engine
                    .new_local_session_context(
                        SessionVars::default(),
                        SessionStorageConfig::default(),
                    )
                    .await?;
                Ok(Connection {
                    sess: Arc::new(Mutex::new(sess)),
                    engine: Arc::new(engine),
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

    /// Run a SQL querying against a GlareDB database.
    ///
    /// Note that this will only plan the query. To execute the query, call any
    /// of `execute`, `show`, `to_arrow`, `to_pandas`, or `to_polars`
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
            let plan = sess.sql_to_lp(query).await.map_err(PyGlareDbError::from)?;
            match plan
                .to_owned()
                .try_into_datafusion_plan()
                .expect("resolving logical plan")
            {
                DFLogicalPlan::Extension(_)
                | DFLogicalPlan::Dml(_)
                | DFLogicalPlan::Ddl(_)
                | DFLogicalPlan::Copy(_) => {
                    sess.execute_inner(plan)
                        .await
                        .map_err(PyGlareDbError::from)?;
                    Ok(PyLogicalPlan::new(LogicalPlan::Noop, cloned_sess))
                }
                _ => Ok(PyLogicalPlan::new(plan, cloned_sess)),
            }
        })
    }

    /// Execute a query.
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
        let exec_result = wait_for_future(py, async move {
            let mut sess = sess.lock().await;
            let plan = sess.sql_to_lp(query).await.map_err(PyGlareDbError::from)?;
            sess.execute_inner(plan).await.map_err(PyGlareDbError::from)
        })?;

        Ok(PyExecutionResult(exec_result))
    }

    /// Close the current session.
    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        wait_for_future(py, async move {
            Ok(self.engine.shutdown().await.map_err(PyGlareDbError::from)?)
        })
    }
}
