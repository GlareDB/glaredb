use std::fmt::Display;

use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::{create_exception, PyErr};

#[derive(Debug, thiserror::Error)]
pub enum PyGlareDbError {
    #[error("{0}")]
    GlareDb(#[from] glaredb::Error),
}

impl PyGlareDbError {
    pub fn new(msg: impl Display) -> Self {
        Self::GlareDb(glaredb::Error::new(msg.to_string()))
    }
}

impl From<PyGlareDbError> for PyErr {
    fn from(err: PyGlareDbError) -> Self {
        match err {
            PyGlareDbError::GlareDb(gerr) => match gerr {
                glaredb::Error::Arrow(err) => ArrowErrorException::new_err(format!("{err:?}")),
                glaredb::Error::Metastore(err) => MetastoreException::new_err(err.to_string()),
                glaredb::Error::Exec(err) => ExecutionException::new_err(err.to_string()),
                glaredb::Error::Anyhow(err) => PyRuntimeError::new_err(format!("{err:?}")),
                glaredb::Error::Other(msg) => PyRuntimeError::new_err(msg),
                glaredb::Error::DataFusion(err) => {
                    DataFusionErrorException::new_err(err.to_string())
                }
                glaredb::Error::ConfigurationBuilder(err) => {
                    ConfigurationException::new_err(err.to_string())
                }
            },
        }
    }
}

create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, ConfigurationException, PyException);
create_exception!(exceptions, DataFusionErrorException, PyException);
create_exception!(exceptions, ExecutionException, PyException);
create_exception!(exceptions, MetastoreException, PyException);
