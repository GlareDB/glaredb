mod errors;
mod event_loop;
mod print;
mod session;

use pyo3::types::{PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, pymodule, wrap_pyfunction};

/// Defines the root python module.
#[pymodule(name = "glaredb")]
fn binding_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(session::connect, m)?)
}
