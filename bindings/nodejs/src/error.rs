use std::fmt::Display;

use napi::bindgen_prelude::*;
pub use napi::Result as JsResult;

#[derive(Debug, thiserror::Error)]
pub enum JsGlareDbError {
    #[error("{0}")]
    GlareDb(#[from] glaredb::Error),
}

impl JsGlareDbError {
    pub fn new(msg: impl Display) -> Self {
        Self::GlareDb(glaredb::Error::Other(msg.to_string()))
    }
}

impl From<JsGlareDbError> for napi::Error {
    fn from(err: JsGlareDbError) -> Self {
        let reason = format!("{}", err);

        napi::Error::from_reason(reason)
    }
}
