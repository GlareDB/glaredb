use crate::types::TypeError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("internal error: {0}")]
    Internal(String),

    #[error("invalid input: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("invalid input: {0}")]
    Parse(ParseError),

    #[error(transparent)]
    TypeError(#[from] TypeError),
}

#[derive(Debug, Clone)]
pub struct ParseError {
    pub(crate) type_name: String,
    pub(crate) input: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "invalid input for type {}: {}",
            self.type_name, self.input
        )
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::error::Error::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
