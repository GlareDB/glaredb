use crate::error::JsGlareDbError;
use crate::logical_plan::JsLogicalPlan;
use datafusion::logical_expr::LogicalPlan as DFLogicalPlan;
use datafusion_ext::vars::SessionVars;
use futures::lock::Mutex;
use sqlexec::engine::{Engine, SessionStorageConfig, TrackedSession};
use sqlexec::remote::client::RemoteClient;
use sqlexec::LogicalPlan;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

pub(super) type JsTrackedSession = Arc<Mutex<TrackedSession>>;

/// A connected session to a GlareDB database.
#[napi]
#[derive(Clone)]
pub struct Connection {
  pub(crate) sess: JsTrackedSession,
  pub(crate) engine: Arc<Engine>,
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
      )
      .await
      .map_err(JsGlareDbError::from)?;

      let mut sess = engine
        .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
        .await
        .map_err(JsGlareDbError::from)?;
      sess
        .attach_remote_session(exec_client.clone(), None)
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
      engine: Arc::new(engine),
    })
  }
  /// Returns a default connection to an in-memory database.
  ///
  /// The database is only initialized once, and all subsequent calls will
  /// return the same connection.
  #[napi]
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
      engine: Arc::new(engine),
    };

    Ok(con.clone())
  }

  /// Run a SQL querying against a GlareDB database.
  ///
  /// Note that this will only plan the query. To execute the query, call any
  /// of `execute`, `show`, `toArrow`,  or `toPolars`
  ///
  /// # Examples
  ///
  /// Show the output of a query.
  ///
  /// ```javascript
  /// import glaredb from "@glaredb/node"
  ///
  /// let con = glaredb.connect()
  /// let cursor = await con.sql('select 1');
  /// await cursor.show()
  /// ```
  ///
  /// Convert the output of a query to a Pandas dataframe.
  ///
  /// ```javascript
  /// import glaredb from "@glaredb/node"
  ///
  /// let con = glaredb.connect()
  /// ```
  ///
  /// Execute the query to completion, returning no output. This is useful
  /// when the query output doesn't matter, for example, creating a table or
  /// inserting data into a table.
  ///
  /// ```javascript
  /// import glaredb from "@glaredb/node"
  ///
  /// con = glaredb.connect()
  /// await con.sql('create table my_table (a int)').then(cursor => cursor.execute())
  /// ```
  #[napi]
  pub async fn sql(&self, query: String) -> napi::Result<JsLogicalPlan> {
    let cloned_sess = self.sess.clone();
    let mut sess = self.sess.lock().await;
    let plan = sess.sql_to_lp(&query).await.map_err(JsGlareDbError::from)?;
    match plan
      .to_owned()
      .try_into_datafusion_plan()
      .expect("resolving logical plan")
    {
      DFLogicalPlan::Extension(_)
      | DFLogicalPlan::Dml(_)
      | DFLogicalPlan::Ddl(_)
      | DFLogicalPlan::Copy(_) => {
        sess
          .execute_inner(plan)
          .await
          .map_err(JsGlareDbError::from)?;
        Ok(JsLogicalPlan::new(LogicalPlan::Noop, cloned_sess))
      }
      _ => Ok(JsLogicalPlan::new(plan, cloned_sess)),
    }
  }

  /// Execute a query.
  ///
  /// # Examples
  ///
  /// Creating a table.
  ///
  /// ```js
  /// import glaredb from "@glaredb/node"
  ///
  /// con = glaredb.connect()
  /// con.execute('create table my_table (a int)')
  /// ```
  #[napi]
  pub async fn execute(&self, query: String) -> napi::Result<()> {
    let sess = self.sess.clone();
    let mut sess = sess.lock().await;
    let plan = sess.sql_to_lp(&query).await.map_err(JsGlareDbError::from)?;
    let _ = sess
      .execute_inner(plan)
      .await
      .map_err(JsGlareDbError::from)?;

    Ok(())
  }

  /// Close the current session.
  #[napi]
  pub async fn close(&self) -> napi::Result<()> {
    Ok(self.engine.shutdown().await.map_err(JsGlareDbError::from)?)
  }
}

/// Ensure that a directory at the given path exists. Errors if the path exists
/// and isn't a directory.
fn ensure_dir(path: impl AsRef<Path>) -> napi::Result<()> {
  let path = path.as_ref();
  if !path.exists() {
    std::fs::create_dir_all(path)?;
  }

  if path.exists() && !path.is_dir() {
    Err(napi::Error::from_reason(format!(
      "Path is not a valid directory {:?}",
      &path
    )))
  } else {
    Ok(())
  }
}
