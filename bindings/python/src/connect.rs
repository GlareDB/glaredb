//! Main entry point for the python bindings.
//!
//! User's will call `connect` which returns a session for executing sql
//! queries.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion_ext::vars::SessionVars;
use futures::lock::Mutex;
use ioutil::ensure_dir;
use pyo3::prelude::*;
use sqlexec::engine::{Engine, EngineBackend, SessionStorageConfig};
use sqlexec::remote::client::{RemoteClient, RemoteClientType};
use url::Url;

use crate::connection::Connection;
use crate::environment::PyEnvironmentReader;
use crate::error::PyGlareDbError;
use crate::runtime::wait_for_future;

#[derive(Debug, Clone)]
struct PythonSessionConf {
    /// Where to store both metastore and user data.
    data_dir: Option<PathBuf>,
    /// URL for cloud deployment to connect to.
    cloud_url: Option<Url>,
}

impl From<Option<String>> for PythonSessionConf {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(s) => match Url::parse(&s) {
                Ok(u) => PythonSessionConf {
                    data_dir: None,
                    cloud_url: Some(u),
                },
                // Assume failing to parse a url just means the user provided a local path.
                Err(_) => PythonSessionConf {
                    data_dir: Some(PathBuf::from(s)),
                    cloud_url: None,
                },
            },
            None => PythonSessionConf {
                data_dir: None,
                cloud_url: None,
            },
        }
    }
}

/// Connect to a GlareDB database.
///
/// # Examples
///
/// Connect to an in-memory database.
///
/// ```python
/// import glaredb
///
/// con = glaredb.connect();
/// ```
///
/// Connect to a database that's persisted to disk.
///
/// ```python
/// import glaredb
///
/// con = glaredb.connect('./my/db');
/// ```
///
/// Connect to a GlareDB Cloud deployment.
///
/// ```python
/// import glaredb
///
/// con = glaredb.connect("glaredb://<user>:<password>@<org>.remote.glaredb.com:6443/<deployment>")
/// ```
#[pyfunction]
#[pyo3(signature = (data_dir_or_cloud_url = None, /, *, spill_path = None, disable_tls = false, cloud_addr = String::from("https://console.glaredb.com"), location = None, storage_options = None))]
pub fn connect(
    py: Python,
    data_dir_or_cloud_url: Option<String>,
    spill_path: Option<String>,
    disable_tls: bool,
    cloud_addr: String,
    location: Option<String>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Connection> {
    wait_for_future(py, async move {
        let conf = PythonSessionConf::from(data_dir_or_cloud_url);

        let backend = if let Some(location) = location.clone() {
            EngineBackend::Remote {
                location,
                options: storage_options.unwrap_or_default(),
            }
        } else if let Some(data_dir) = conf.data_dir.clone() {
            EngineBackend::Local(data_dir)
        } else {
            EngineBackend::Memory
        };

        let mut engine = Engine::from_backend(backend)
            .await
            .map_err(PyGlareDbError::from)?;

        // If spill path not provided, default to some tmp dir.
        let spill_path = match spill_path {
            Some(p) => {
                let path = PathBuf::from(p);
                ensure_dir(&path)?;
                Some(path)
            }
            None => {
                let path = std::env::temp_dir().join("glaredb-python");
                // if user doesn't have permission to write to temp dir, then
                // just don't use a spill path.
                ensure_dir(&path).ok().map(|_| path)
            }
        };
        engine = engine.with_spill_path(spill_path);

        let mut session = if let Some(url) = conf.cloud_url.clone() {
            let exec_client = RemoteClient::connect_with_proxy_destination(
                url.try_into().map_err(PyGlareDbError::from)?,
                cloud_addr,
                disable_tls,
                RemoteClientType::Python,
            )
            .await
            .map_err(PyGlareDbError::from)?;

            let mut sess = engine
                .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
                .await
                .map_err(PyGlareDbError::from)?;
            sess.attach_remote_session(exec_client.clone(), None)
                .await
                .map_err(PyGlareDbError::from)?;

            sess
        } else {
            engine
                .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
                .await
                .map_err(PyGlareDbError::from)?
        };

        session.register_env_reader(Box::new(PyEnvironmentReader));
        let sess = Arc::new(Mutex::new(session));

        Ok(Connection {
            sess,
            _engine: Arc::new(engine),
        })
    })
}
