use std::sync::Arc;

use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::PyType;
use sqlexec::remote::client::RemoteClientType;

use crate::environment::PyEnvironmentReader;
use crate::error::PyGlareDbError;
use crate::execution::PyExecutionOutput;
use crate::runtime::wait_for_future;

/// A connected session to a GlareDB database.
#[pyclass]
#[derive(Clone)]
pub struct Connection {
    pub(crate) inner: Arc<glaredb::Connection>,
}

impl Connection {
    /// Returns a default connection to an in-memory database.
    ///
    /// This database is only initialized once, and all subsequent
    /// calls will return the same connection object and therefore
    /// access the same data and database.
    pub fn default_in_memory(py: Python<'_>) -> PyResult<Self> {
        static DEFAULT_CON: OnceCell<Connection> = OnceCell::new();

        let con = DEFAULT_CON.get_or_try_init(|| {
            wait_for_future(py, async move {
                Ok(Connection {
                    inner: Arc::new(
                        glaredb::ConnectOptionsBuilder::new_in_memory()
                            .client_type(RemoteClientType::Python)
                            .environment_reader(Arc::new(PyEnvironmentReader))
                            .build()?
                            .connect()
                            .await?,
                    ),
                }) as Result<_, PyGlareDbError>
            })
        })?;

        Ok(con.clone())
    }
}

#[pymethods]
impl Connection {
    fn __enter__(&self, _py: Python<'_>) -> PyResult<Self> {
        Ok(self.clone())
    }

    fn __exit__(
        &self,
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
    pub fn sql(&self, py: Python<'_>, query: &str) -> PyResult<PyExecutionOutput> {
        wait_for_future(py, async move {
            Ok(self
                .inner
                .sql(query)
                .evaluate()
                .await
                .map_err(PyGlareDbError::from)?
                .into())
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
    pub fn prql(&self, py: Python<'_>, query: &str) -> PyResult<PyExecutionOutput> {
        wait_for_future(py, async move {
            Ok(self
                .inner
                .prql(query)
                .evaluate()
                .await
                .map_err(PyGlareDbError::from)?
                .into())
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
    pub fn execute(&self, py: Python<'_>, query: &str) -> PyResult<PyExecutionOutput> {
        wait_for_future(py, async move {
            Ok(self
                .inner
                .execute(query)
                .evaluate()
                .await
                .map_err(PyGlareDbError::from)?
                .into())
        })
    }

    /// Close the current session.
    pub fn close(&self, _py: Python<'_>) -> PyResult<()> {
        // TODO: Remove this method. No longer required.
        //
        // could we use this method to clear the environment/in memory
        // database?
        Ok(())
    }
}
