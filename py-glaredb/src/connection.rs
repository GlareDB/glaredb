use crate::execution_result::PyExecutionResult;
use futures::lock::Mutex;
use pyo3::{prelude::*, types::PyType};
use sqlexec::engine::{Engine, TrackedSession};
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
    fn sql(&mut self, py: Python<'_>, query: &str) -> PyResult<PyLogicalPlan> {
        let cloned_sess = self.sess.clone();
        wait_for_future(py, async move {
            let mut sess = self.sess.lock().await;
            Ok(sess
                .sql_to_lp(query)
                .await
                .map(|lp| PyLogicalPlan::new(lp, cloned_sess))
                .map_err(PyGlareDbError::from)?)
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
    fn execute(&mut self, py: Python<'_>, query: &str) -> PyResult<PyExecutionResult> {
        let sess = self.sess.clone();
        let exec_result = wait_for_future(py, async move {
            let mut sess = sess.lock().await;
            let plan = sess.sql_to_lp(query).await.map_err(PyGlareDbError::from)?;
            sess.execute_inner(plan).await.map_err(PyGlareDbError::from)
        })?;

        Ok(PyExecutionResult(exec_result))
    }

    /// Close the current session.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        wait_for_future(py, async move {
            Ok(self.engine.shutdown().await.map_err(PyGlareDbError::from)?)
        })
    }
}
