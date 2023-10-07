use crate::execution_result::PyExecutionResult;
use crate::util::pyprint;
use anyhow::Result;
use arrow_util::pretty::pretty_format_batches;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::arrow::record_batch::RecordBatch;
use futures::lock::Mutex;
use futures::StreamExt;
use pgrepr::format::Format;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyTuple};
use sqlexec::{
    engine::{Engine, TrackedSession},
    parser,
    session::ExecutionResult,
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
            Ok(sess
                .sql_to_lp(query)
                .await
                .map(|lp| PyLogicalPlan::new(lp, cloned_sess))
                .map_err(PyGlareDbError::from)?)
        })
    }

    /// Execute a query to completion.
    ///
    /// This is equivalent to calling `execute` on the result of a `sql` call.
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
    fn execute(&mut self, py: Python<'_>, query: &str) -> PyResult<()> {
        wait_for_future(py, async move {
            let mut sess = self.sess.lock().await;
            let plan = sess.sql_to_lp(query).await.map_err(PyGlareDbError::from)?;
            let out = sess
                .execute_inner(plan)
                .await
                .map_err(PyGlareDbError::from)?;
            PyExecutionResult(out).execute(py)?;
            Ok(())
        })
    }

    /// Close the current session.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        wait_for_future(py, async move {
            if let Err(err) = self.sess.lock().await.close().await {
                eprintln!("unable to close the existing session: {err}");
            }
            Ok(self.engine.shutdown().await.map_err(PyGlareDbError::from)?)
        })
    }
}
