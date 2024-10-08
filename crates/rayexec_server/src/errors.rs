use std::error::Error;
use std::fmt;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rayexec_error::RayexecError;

pub type ServerResult<T, E = ServerError> = std::result::Result<T, E>;

/// Wrapper around a rayexec error that can be converted into a response.
#[derive(Debug)]
pub struct ServerError {
    pub error: RayexecError,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}

impl From<RayexecError> for ServerError {
    fn from(value: RayexecError) -> Self {
        ServerError { error: value }
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl Error for ServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.error.source()
    }
}
