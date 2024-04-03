use std::fmt::Display;

use datafusion::arrow::error::ArrowError;
use glaredb::DataFusionError;
use metastore::errors::MetastoreError;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::{create_exception, PyErr};
use sqlexec::errors::ExecError;

#[derive(Debug, thiserror::Error)]
pub enum PyGlareDbError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error(transparent)]
    PyErr(#[from] PyErr),

    #[error(transparent)]
    Metastore(#[from] MetastoreError),
    #[error(transparent)]
    Exec(#[from] ExecError),
    #[error(transparent)]
    ConfigurationBuilder(#[from] glaredb::ConnectOptionsBuilderError),

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
        match err {
            PyGlareDbError::Arrow(err) => ArrowErrorException::new_err(format!("{err:?}")),
            PyGlareDbError::Metastore(err) => MetastoreException::new_err(err.to_string()),
            PyGlareDbError::Exec(err) => ExecutionException::new_err(err.to_string()),
            PyGlareDbError::Anyhow(err) => PyRuntimeError::new_err(format!("{err:?}")),
            PyGlareDbError::Other(msg) => PyRuntimeError::new_err(msg),
            PyGlareDbError::DataFusion(err) => DataFusionErrorException::new_err(err.to_string()),
            PyGlareDbError::PyErr(err) => err,
            PyGlareDbError::ConfigurationBuilder(err) => {
                ConfigurationException::new_err(err.to_string())
            }
        }
    }
}

create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, ConfigurationException, PyException);
create_exception!(exceptions, DataFusionErrorException, PyException);
create_exception!(exceptions, ExecutionException, PyException);
create_exception!(exceptions, MetastoreException, PyException);
