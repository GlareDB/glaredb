use std::fmt::Display;

use napi::bindgen_prelude::*;
pub use napi::Result as JsResult;

#[derive(Debug, thiserror::Error)]
pub enum JsDatabaseError {
    #[error("{0}")]
    Database(#[from] glaredb::DatabaseError),
}

impl JsDatabaseError {
    pub fn new(msg: impl Display) -> Self {
        Self::Database(glaredb::DatabaseError::new(msg.to_string()))
    }
}

impl From<JsDatabaseError> for napi::Error {
    fn from(err: JsDatabaseError) -> Self {
        let reason = format!("{}", err);

        napi::Error::from_reason(reason)
    }
}
