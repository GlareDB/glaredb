use pyo3::prelude::*;

#[pyclass]
pub(crate) struct TokioRuntime(pub(crate) tokio::runtime::Runtime);
