mod connect;
mod connection;
mod environment;
mod error;
mod execution_result;
mod logical_plan;
mod runtime;
mod util;

use pyo3::prelude::*;
use runtime::TokioRuntime;
use std::sync::atomic::{AtomicU64, Ordering};
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

    m.add_function(wrap_pyfunction!(connect::connect, m)?)?;
    Ok(())
}
