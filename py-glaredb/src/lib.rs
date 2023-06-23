pub mod runtime;
mod utils;
mod session;
use runtime::TokioRuntime;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

use metastore::local::start_inprocess_local;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use sqlexec::engine::{
    Engine, EngineStorageConfig, SessionLimits, SessionStorageConfig, TrackedSession,
};
use telemetry::Tracker;

use crate::utils::wait_for_future;

#[pyclass]
pub struct LocalSession {
    sess: TrackedSession,
    _engine: Engine, // Avoid dropping
                     // opts: LocalClientOpts,
}

pub struct LocalClientOpts {
    /// Address to the Metastore.
    ///
    /// If not provided, an in-process metastore will be started.
    pub metastore_addr: Option<String>,

    /// Path to spill temporary files to.
    pub spill_path: Option<PathBuf>,

    /// Optional file path for persisting data.
    ///
    /// Catalog data and user data will be stored in this directory.
    pub data_dir: Option<PathBuf>,
}

fn get_or_create_path(path: &str) -> PyResult<&Path> {
    let path = Path::new(path);
    if !path.exists() {
        fs::create_dir_all(&path)?;
    }
    if path.exists() && !path.is_dir() {
        return Err(PyRuntimeError::new_err(format!(
            "Path is not a valid directory {:?}",
            &path
        )));
    } else {
        Ok(path)
    }
}

#[pyfunction]
fn connect(py: Python, data_dir: String, _spill_path: Option<String>) -> PyResult<LocalSession> {
    let session = wait_for_future(py, async move {
        let tracker = Arc::new(Tracker::Nop);
        let path = get_or_create_path(&data_dir).unwrap();

        let storage_conf = EngineStorageConfig::Memory;
        let metastore_client = start_inprocess_local(path).await.unwrap();
        let engine = Engine::new(metastore_client, storage_conf, tracker, None)
            .await
            .unwrap();
        let session = engine
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
            .unwrap();
        LocalSession {
            sess: session,
            _engine: engine,
        }
    });
    Ok(session)
}

/// A Python module implemented in Rust.
#[pymodule]
fn glaredb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add(
        "__runtime",
        TokioRuntime(tokio::runtime::Runtime::new().unwrap()),
    )?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
