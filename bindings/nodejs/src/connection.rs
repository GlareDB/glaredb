use std::sync::Arc;

use async_once_cell::OnceCell;

use crate::error::JsGlareDbError;
use crate::execution::JsExecutionOutput;

/// A connected session to a GlareDB database.
#[napi]
#[derive(Clone)]
pub struct Connection {
    pub(crate) inner: Arc<glaredb::Connection>,
}

#[napi]
impl Connection {
    /// Returns the default connection to a global in-memory database.
    ///
    /// The database is only initialized once, and all subsequent
    /// calls will return the same connection object and therefore
    /// have access to the same data.
    #[napi(catch_unwind)]
    pub async fn default_in_memory() -> napi::Result<Connection> {
        static DEFAULT_CON: OnceCell<Connection> = OnceCell::new();

        Ok(DEFAULT_CON
            .get_or_try_init(async {
                Ok::<_, JsGlareDbError>(Connection {
                    inner: Arc::new(
                        glaredb::ConnectOptionsBuilder::new_in_memory()
                            .build()?
                            .connect()
                            .await?,
                    ),
                })
            })
            .await?
            .clone())
    }

    /// Run a SQL operation against a GlareDB database.
    ///
    /// All operations that write or modify data are executed
    /// directly, but all query operations run lazily when you process
    /// their results with `show`, `toArrow`, or
    /// `toPolars`, or call the `execute` method.
    ///
    /// # Examples
    ///
    /// Show the output of a query.
    ///
    /// ```javascript
    /// import glaredb from "@glaredb/glaredb"
    ///
    /// let con = glaredb.connect()
    /// let cursor = await con.sql('select 1');
    /// await cursor.show()
    /// ```
    ///
    /// Convert the output of a query to a Pandas dataframe.
    ///
    /// ```javascript
    /// import glaredb from "@glaredb/glaredb"
    ///
    /// let con = glaredb.connect()
    /// ```
    ///
    /// Execute the query to completion, returning no output. This is useful
    /// when the query output doesn't matter, for example, creating a table or
    /// inserting data into a table.
    ///
    /// ```javascript
    /// import glaredb from "@glaredb/glaredb"
    ///
    /// con = glaredb.connect()
    /// await con.sql('create table my_table (a int)').then(cursor => cursor.execute())
    /// ```
    #[napi(catch_unwind)]
    pub async fn sql(&self, query: String) -> napi::Result<JsExecutionOutput> {
        Ok(self
            .inner
            .sql(query)
            .evaluate()
            .await
            .map_err(JsGlareDbError::from)?
            .into())
    }

    /// Run a PRQL query against a GlareDB database. Does not change
    /// the state or dialect of the connection object.
    ///
    /// ```javascript
    /// import glaredb from "@glaredb/glaredb"
    ///
    /// let con = glaredb.connect()
    /// let cursor = await con.prql('from my_table | take 1');
    /// await cursor.show()
    /// ```
    ///
    /// All operations execute lazily when their results are
    /// processed.
    #[napi(catch_unwind)]
    pub async fn prql(&self, query: String) -> napi::Result<JsExecutionOutput> {
        Ok(self
            .inner
            .prql(query)
            .evaluate()
            .await
            .map_err(JsGlareDbError::from)?
            .into())
    }

    /// Execute a query.
    ///
    /// # Examples
    ///
    /// Creating a table.
    ///
    /// ```js
    /// import glaredb from "@glaredb/glaredb"
    ///
    /// con = glaredb.connect()
    /// con.execute('create table my_table (a int)')
    /// ```
    #[napi(catch_unwind)]
    pub async fn execute(&self, query: String) -> napi::Result<()> {
        self.inner
            .execute(query)
            .call()
            .check()
            .await
            .map_err(JsGlareDbError::from)?;

        Ok(())
    }

    /// Close the current session.
    #[napi(catch_unwind)]
    pub async fn close(&self) -> napi::Result<()> {
        // TODO: Remove this method. No longer required.
        Ok(())
    }
}
