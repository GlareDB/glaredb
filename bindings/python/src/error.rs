use std::fmt::Display;

use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::{create_exception, PyErr};

#[derive(Debug, thiserror::Error)]
pub enum PyDatabaseError {
    #[error("{0}")]
    Database(#[from] glaredb::DatabaseError),
}

impl PyDatabaseError {
    pub fn new(msg: impl Display) -> Self {
        Self::Database(glaredb::DatabaseError::new(msg.to_string()))
    }
}

impl From<PyDatabaseError> for PyErr {
    fn from(err: PyDatabaseError) -> Self {
        match err {
            PyDatabaseError::Database(gerr) => match gerr {
                glaredb::DatabaseError::Arrow(err) => {
                    ArrowErrorException::new_err(format!("{err:?}"))
                }
                glaredb::DatabaseError::Metastore(err) => {
                    MetastoreException::new_err(err.to_string())
                }
                glaredb::DatabaseError::Exec(err) => ExecutionException::new_err(err.to_string()),
                glaredb::DatabaseError::Anyhow(err) => PyRuntimeError::new_err(format!("{err:?}")),
                glaredb::DatabaseError::Other(msg) => PyRuntimeError::new_err(msg),
                glaredb::DatabaseError::DataFusion(err) => {
                    DataFusionErrorException::new_err(err.to_string())
                }
                glaredb::DatabaseError::ConfigurationBuilder(err) => {
                    ConfigurationException::new_err(err.to_string())
                }
                glaredb::DatabaseError::CannotResolveUnevaluatedOperation
                | glaredb::DatabaseError::UnsupportedLazyEvaluation => {
                    PyRuntimeError::new_err(gerr.to_string())
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
