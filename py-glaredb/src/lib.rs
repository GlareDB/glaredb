mod environment;
mod error;
mod runtime;
mod session;

use environment::PyEnvironmentReader;
use error::PyGlareDbError;
use runtime::{wait_for_future, TokioRuntime};
use session::LocalSession;
use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::runtime::Builder;
use uuid::Uuid;

use metastore::local::start_inprocess_local;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use sqlexec::engine::{Engine, EngineStorageConfig, SessionLimits, SessionStorageConfig};
use telemetry::Tracker;

fn get_or_create_path(path: &str) -> PyResult<&Path> {
    let path = Path::new(path);

    if !path.exists() {
        fs::create_dir_all(path)?;
    }

    if path.exists() && !path.is_dir() {
        Err(PyRuntimeError::new_err(format!(
            "Path is not a valid directory {:?}",
            &path
        )))
    } else {
        Ok(path)
    }
}

#[pyfunction]
fn connect(py: Python, data_dir: String, _spill_path: Option<String>) -> PyResult<LocalSession> {
    wait_for_future(py, async move {
        let tracker = Arc::new(Tracker::Nop);
        let path = get_or_create_path(&data_dir).unwrap();

        let storage_conf = EngineStorageConfig::Memory;
        let metastore_client = start_inprocess_local(path)
            .await
            .map_err(PyGlareDbError::from)?;

        let engine = Engine::new(metastore_client, storage_conf, tracker, None)
            .await
            .map_err(PyGlareDbError::from)?;

        let mut session = engine
            .new_session(
                Uuid::nil(),
                "glaredb".to_string(),
                Uuid::nil(),
                Uuid::nil(),
                "glaredb".to_string(),
                SessionLimits::default(),
                SessionStorageConfig::default(),
            )
            .await
            .map_err(PyGlareDbError::from)?;

        session.register_env_reader(Box::new(PyEnvironmentReader));

        Ok(LocalSession {
            sess: session,
            _engine: engine,
        })
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
