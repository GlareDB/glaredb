use crate::execution_result::PyExecutionResult;
use datafusion::logical_expr::LogicalPlan as DFLogicalPlan;
use futures::lock::Mutex;
use pyo3::prelude::*;
use sqlexec::{
    engine::{Engine, TrackedSession},
    LogicalPlan,
};
use std::sync::Arc;

pub(super) type PyTrackedSession = Arc<Mutex<TrackedSession>>;

use crate::{error::PyGlareDbError, logical_plan::PyLogicalPlan, runtime::wait_for_future};

/// A connected session to a GlareDB database.
#[pyclass]
pub struct LocalSession {
    pub(super) sess: PyTrackedSession,
    pub(super) engine: Engine,
}

#[pymethods]
impl LocalSession {
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
