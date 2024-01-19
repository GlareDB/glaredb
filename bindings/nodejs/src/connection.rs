use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan as DFLogicalPlan;
use datafusion_ext::vars::SessionVars;
use futures::lock::Mutex;
use ioutil::ensure_dir;
use sqlexec::engine::{Engine, SessionStorageConfig, TrackedSession};
use sqlexec::remote::client::{RemoteClient, RemoteClientType};
use sqlexec::{LogicalPlan, OperationInfo};
use url::Url;

use crate::error::JsGlareDbError;
use crate::logical_plan::JsLogicalPlan;

pub(super) type JsTrackedSession = Arc<Mutex<TrackedSession>>;

/// A connected session to a GlareDB database.
#[napi]
#[derive(Clone)]
pub struct Connection {
    pub(crate) sess: JsTrackedSession,
    pub(crate) _engine: Arc<Engine>,
}

#[derive(Debug, Clone)]
struct JsSessionConf {
    /// Where to store both metastore and user data.
    data_dir: Option<PathBuf>,
    /// URL for cloud deployment to connect to.
    cloud_url: Option<Url>,
}

impl From<Option<String>> for JsSessionConf {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(s) => match Url::parse(&s) {
                Ok(u) => JsSessionConf {
                    data_dir: None,
                    cloud_url: Some(u),
                },
                // Assume failing to parse a url just means the user provided a local path.
                Err(_) => JsSessionConf {
                    data_dir: Some(PathBuf::from(s)),
                    cloud_url: None,
                },
            },
            None => JsSessionConf {
                data_dir: None,
                cloud_url: None,
            },
        }
    }
}

#[napi]
impl Connection {
    pub(crate) async fn connect(
        data_dir_or_cloud_url: Option<String>,
        spill_path: Option<String>,
        disable_tls: bool,
        cloud_addr: String,
        location: Option<String>,
        storage_options: Option<HashMap<String, String>>,
    ) -> napi::Result<Self> {
        let conf = JsSessionConf::from(data_dir_or_cloud_url);

        let mut engine = if let Some(location) = location {
            // TODO: try to consolidate with --data-dir option
            Engine::from_storage_options(&location, &storage_options.unwrap_or_default())
                .await
                .map_err(JsGlareDbError::from)?
        } else {
            // If data dir is provided, then both table storage and metastore
            // storage will reside at that path. Otherwise everything is in memory.
            Engine::from_data_dir(conf.data_dir.as_ref())
                .await
                .map_err(JsGlareDbError::from)?
        };

        // If spill path not provided, default to some tmp dir.
        let spill_path = match spill_path {
            Some(p) => {
                let path = PathBuf::from(p);
                ensure_dir(&path)?;
                Some(path)
            }
            None => {
                let path = std::env::temp_dir().join("glaredb-js");
                // if user doesn't have permission to write to temp dir, then
                // just don't use a spill path.
                ensure_dir(&path).ok().map(|_| path)
            }
        };
        engine = engine.with_spill_path(spill_path);

        let session = if let Some(url) = conf.cloud_url.clone() {
            let exec_client = RemoteClient::connect_with_proxy_destination(
                url.try_into().map_err(JsGlareDbError::from)?,
                cloud_addr,
                disable_tls,
                RemoteClientType::Node,
            )
            .await
            .map_err(JsGlareDbError::from)?;

            let mut sess = engine
                .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
                .await
                .map_err(JsGlareDbError::from)?;
            sess.attach_remote_session(exec_client.clone(), None)
                .await
                .map_err(JsGlareDbError::from)?;

            sess
        } else {
            engine
                .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
                .await
                .map_err(JsGlareDbError::from)?
        };

        let sess = Arc::new(Mutex::new(session));

        Ok(Connection {
            sess,
            _engine: Arc::new(engine),
        })
    }

    /// Returns a default connection to an in-memory database.
    ///
    /// The database is only initialized once, and all subsequent calls will
    /// return the same connection.
    #[napi(catch_unwind)]
    pub async fn default_in_memory() -> napi::Result<Connection> {
        let engine = Engine::from_data_dir(None)
            .await
            .map_err(JsGlareDbError::from)?;
        let sess = engine
            .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
            .await
            .map_err(JsGlareDbError::from)?;
        let con = Connection {
            sess: Arc::new(Mutex::new(sess)),
            _engine: Arc::new(engine),
        };

        Ok(con.clone())
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
    pub async fn sql(&self, query: String) -> napi::Result<JsLogicalPlan> {
        let cloned_sess = self.sess.clone();
        let mut sess = self.sess.lock().await;

        let plan = sess
            .create_logical_plan(&query)
            .await
            .map_err(JsGlareDbError::from)?;

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
                    .map_err(JsGlareDbError::from)?;

                Ok(JsLogicalPlan::new(
                    LogicalPlan::Noop,
                    cloned_sess,
                    Default::default(),
                ))
            }
            _ => Ok(JsLogicalPlan::new(plan, cloned_sess, op)),
        }
    }

    /// Run a PRQL query against a GlareDB database. Does not change
    /// the state or dialect of the connection object.
    ///
    /// ```javascript
    /// import glaredb from "@glaredb/glaredb"
    ///
    /// let con = glaredb.connect()
    /// let cursor = await con.sql('from my_table | take 1');
    /// await cursor.show()
    /// ```
    ///
    /// All operations execute lazily when their results are
    /// processed.
    #[napi(catch_unwind)]
    pub async fn prql(&self, query: String) -> napi::Result<JsLogicalPlan> {
        let cloned_sess = self.sess.clone();
        let mut sess = self.sess.lock().await;
        let plan = sess
            .prql_to_lp(&query)
            .await
            .map_err(JsGlareDbError::from)?;

        let op = OperationInfo::new().with_query_text(query);

        Ok(JsLogicalPlan::new(plan, cloned_sess, op))
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
        let sess = self.sess.clone();
        let mut sess = sess.lock().await;

        let plan = sess
            .create_logical_plan(&query)
            .await
            .map_err(JsGlareDbError::from)?;

        let op = OperationInfo::new().with_query_text(query);

        let _ = sess
            .execute_logical_plan(plan, &op)
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
