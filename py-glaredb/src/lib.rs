mod environment;
mod error;
mod logical_plan;
mod runtime;
mod session;
use environment::PyEnvironmentReader;
use error::PyGlareDbError;
use futures::lock::Mutex;
use runtime::{wait_for_future, TokioRuntime};
use session::LocalSession;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::runtime::Builder;

use datafusion_ext::vars::SessionVars;
use metastore::local::{start_inprocess_inmemory, start_inprocess_local};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use sqlexec::engine::{Engine, EngineStorageConfig, SessionStorageConfig};

use telemetry::Tracker;

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

/// Create and connect to a GlareDB engine.
// TODO: kwargs
#[pyfunction]
fn connect(
    py: Python,
    data_dir: Option<String>,
    spill_path: Option<String>,
) -> PyResult<LocalSession> {
    wait_for_future(py, async move {
        let tracker = Arc::new(Tracker::Nop);

        // If data dir is provided, then both table storage and metastore storage
        // will reside at that path. Otherwise everything is in memory.
        //
        // TODO: Possibly support connecting to remote metastore and storage?
        let (storage_conf, metastore_client) = match data_dir {
            Some(path) => {
                let path = PathBuf::from(path);
                ensure_dir(&path)?;
                let metastore_client = start_inprocess_local(&path)
                    .await
                    .map_err(PyGlareDbError::from)?;
                let conf = EngineStorageConfig::Local { path };

                (conf, metastore_client)
            }
            None => {
                let metastore_client = start_inprocess_inmemory()
                    .await
                    .map_err(PyGlareDbError::from)?;
                let conf = EngineStorageConfig::Memory;
                (conf, metastore_client)
            }
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

        let engine = Engine::new(metastore_client, storage_conf, tracker, spill_path)
            .await
            .map_err(PyGlareDbError::from)?;

        let mut session = engine
            .new_session(SessionVars::default(), SessionStorageConfig::default())
            .await
            .map_err(PyGlareDbError::from)?;

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
