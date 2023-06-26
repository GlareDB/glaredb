use std::future::Future;

use pyo3::{prelude::*, PyRef, Python};
use tokio::runtime::Runtime;

#[pyclass]
pub(crate) struct TokioRuntime(pub(crate) tokio::runtime::Runtime);

/// Utility to get the Tokio Runtime from Python
pub(crate) fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    let glaredb = py.import("glaredb.glaredb").unwrap();
    glaredb.getattr("__runtime").unwrap().extract().unwrap()
}

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime(py).0;
    py.allow_threads(|| runtime.block_on(f))
}
