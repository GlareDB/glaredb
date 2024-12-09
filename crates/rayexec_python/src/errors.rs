use std::error::Error;
use std::fmt;

use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use rayexec_error::RayexecError;

pub type Result<T, E = PythonError> = std::result::Result<T, E>;

/// Wrapper around a rayexec error to convert into a `PyErr`.
#[derive(Debug)]
pub enum PythonError {
    Rayexec(RayexecError),
    PyErr(PyErr),
}

impl Error for PythonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rayexec(err) => err.source(),
            Self::PyErr(err) => err.source(),
        }
    }
}

impl fmt::Display for PythonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rayexec(err) => err.fmt(f),
            Self::PyErr(err) => err.fmt(f),
        }
    }
}

impl From<RayexecError> for PythonError {
    fn from(value: RayexecError) -> Self {
        PythonError::Rayexec(value)
    }
}

impl From<PyErr> for PythonError {
    fn from(value: PyErr) -> Self {
        PythonError::PyErr(value)
    }
}

impl From<PythonError> for PyErr {
    fn from(value: PythonError) -> Self {
        match value {
            PythonError::Rayexec(error) => PyRuntimeError::new_err(error.to_string()),
            PythonError::PyErr(error) => error,
        }
    }
}
