#![allow(clippy::wrong_self_convention)] // this is consistent with other python API's

mod connect;
mod connection;
mod environment;
mod error;
mod execution;
mod runtime;
mod util;

use std::sync::atomic::{AtomicU64, Ordering};

use connection::Connection;
use execution::PyExecution;
use pyo3::prelude::*;
use runtime::TokioRuntime;
use tokio::runtime::Builder;

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

    m.add_function(wrap_pyfunction!(sql, m)?)?;
    m.add_function(wrap_pyfunction!(execute, m)?)?;

    m.add_function(wrap_pyfunction!(connect::connect, m)?)?;

    Ok(())
}

/// Run a SQL query against the default in-memory GlareDB
/// database. Subsequent calls to this method will always interact
/// with the same underlying connection object and therefore access
/// the same data and database.
#[pyfunction]
pub fn sql(py: Python, query: &str) -> PyResult<PyExecution> {
    Connection::default_in_memory(py)?.sql(py, query)
}

/// Run a PRQL query against the default in-memory GlareDB
/// database. Subsequent calls to this method will always interact
/// with the same underlying connection object and therefore access
/// the same data and database.
#[pyfunction]
pub fn prql(py: Python, query: &str) -> PyResult<PyExecution> {
    Connection::default_in_memory(py)?.prql(py, query)
}


/// Execute a query against the default in-memory GlareDB
/// database. Subsequent calls to this method will always interact
/// with the same underlying connection object and therefore access
/// the same data and database.
#[pyfunction]
pub fn execute(py: Python, query: &str) -> PyResult<PyExecution> {
    Connection::default_in_memory(py)?.execute(py, query)
}
