mod environment;
mod error;
mod logical_plan;
mod runtime;
mod session;
mod util;

use environment::PyEnvironmentReader;
use error::PyGlareDbError;
use futures::lock::Mutex;
use runtime::{wait_for_future, TokioRuntime};
use session::LocalSession;
use std::collections::HashMap;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::runtime::Builder;
use url::Url;

use datafusion_ext::vars::SessionVars;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use sqlexec::{
    engine::{Engine, SessionStorageConfig},
    remote::client::RemoteClient,
};

/// Ensure that a directory at the given path exists. Errors if the path exists
/// and isn't a directory.
fn ensure_dir(path: impl AsRef<Path>) -> PyResult<()> {
    let path = path.as_ref();
    if !path.exists() {
        fs::create_dir_all(path)?;
    }

    if path.exists() && !path.is_dir() {
        Err(PyRuntimeError::new_err(format!(
            "Path is not a valid directory {:?}",
            &path
        )))
    } else {
        Ok(())
    }
}

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

#[pyfunction]
#[pyo3(signature = (data_dir_or_cloud_url = None, /, *, spill_path = None, disable_tls = true, cloud_addr = String::from("https://console.glaredb.com"), location = None, storage_options = None))]
fn connect(
    py: Python,
    data_dir_or_cloud_url: Option<String>,
    spill_path: Option<String>,
    disable_tls: bool,
    cloud_addr: String,
    location: Option<String>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<LocalSession> {
    wait_for_future(py, async move {
        let conf = PythonSessionConf::from(data_dir_or_cloud_url);

        let mut engine = if let Some(location) = location {
            // TODO: try to consolidate with --data-dir option
            Engine::from_storage_options(&location, &storage_options.unwrap_or_default())
                .await
                .map_err(PyGlareDbError::from)?
        } else {
            // If data dir is provided, then both table storage and metastore
            // storage will reside at that path. Otherwise everything is in memory.
            Engine::from_data_dir(&conf.data_dir)
                .await
                .map_err(PyGlareDbError::from)?
        };

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

        Ok(LocalSession { sess, engine })
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn glaredb(_py: Python, m: &PyModule) -> PyResult<()> {
    // add the Tokio runtime to the module so we can access it later
    let runtime = Builder::new_multi_thread()
        .thread_name_fn(move || {
            static THREAD_ID: AtomicU64 = AtomicU64::new(0);
            let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
            format!("glaredb-python-thread-{}", id)
        })
        .enable_all()
        .build()
        .unwrap();

    m.add("__runtime", TokioRuntime(runtime))?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
