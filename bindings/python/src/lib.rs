#![allow(clippy::wrong_self_convention)] // this is consistent with other python API's

mod connect;
mod connection;
mod environment;
mod error;
mod execution_result;
mod logical_plan;
mod runtime;
mod util;

use std::sync::atomic::{AtomicU64, Ordering};

use connection::Connection;
use execution_result::PyExecutionResult;
use logical_plan::PyLogicalPlan;
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
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}

/// Run a SQL query against an in-memory GlareDB database.
#[pyfunction]
pub fn sql(py: Python, query: &str) -> PyResult<PyLogicalPlan> {
    let mut con = Connection::default_in_memory(py)?;
    con.sql(py, query)
}

/// Execute a query against an in-memory GlareDB database.
#[pyfunction]
pub fn execute(py: Python, query: &str) -> PyResult<PyExecutionResult> {
    let mut con = Connection::default_in_memory(py)?;
    con.execute(py, query)
}
