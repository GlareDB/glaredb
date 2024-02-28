use std::fmt::Display;

use datafusion::arrow::error::ArrowError;
use metastore::errors::MetastoreError;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::{create_exception, PyErr};
use sqlexec::errors::ExecError;

#[derive(Debug, thiserror::Error)]
pub enum PyGlareDbError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Metastore(#[from] MetastoreError),
    #[error(transparent)]
    Exec(#[from] ExecError),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error("{0}")]
    Other(String),
}

impl PyGlareDbError {
    pub fn new(msg: impl Display) -> Self {
        Self::Other(msg.to_string())
    }
}
impl From<PyGlareDbError> for PyErr {
    fn from(err: PyGlareDbError) -> Self {
        use PyGlareDbError::*;
        match err {
            Arrow(err) => ArrowErrorException::new_err(format!("{err:?}")),
            Metastore(err) => MetastoreException::new_err(err.to_string()),
            Exec(err) => ExecutionException::new_err(err.to_string()),
            Anyhow(err) => PyRuntimeError::new_err(format!("{err:?}")),
            Other(msg) => PyRuntimeError::new_err(msg),
        }
    }
}

create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, MetastoreException, PyException);
create_exception!(exceptions, ExecutionException, PyException);
