use std::fmt::Display;

use datafusion::arrow::error::ArrowError;
use metastore::errors::MetastoreError;
use napi::bindgen_prelude::*;
pub use napi::Result as JsResult;
use sqlexec::errors::ExecError;

#[derive(Debug, thiserror::Error)]
pub enum JsGlareDbError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Metastore(#[from] MetastoreError),
    #[error(transparent)]
    Exec(#[from] ExecError),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("{0}")]
    Other(String),
}

impl JsGlareDbError {
    pub fn new(msg: impl Display) -> Self {
        Self::Other(msg.to_string())
    }
}

impl From<JsGlareDbError> for napi::Error {
    fn from(err: JsGlareDbError) -> Self {
        let reason = format!("{}", err);

        napi::Error::from_reason(reason)
    }
}
