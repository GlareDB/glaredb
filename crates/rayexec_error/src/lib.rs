use std::backtrace::{Backtrace, BacktraceStatus};
use std::error::Error;
use std::fmt;

pub type Result<T, E = RayexecError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct RayexecError {
    /// Message for the error.
    pub msg: String,

    /// Source of the error.
    pub source: Option<Box<dyn Error>>,

    /// Captured backtrace for the error.
    ///
    /// Enable with the RUST_BACKTRACE env var.
    pub backtrace: Backtrace,
}

impl RayexecError {
    pub fn new(msg: impl Into<String>) -> Self {
        RayexecError {
            msg: msg.into(),
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn with_source(msg: impl Into<String>, source: Box<dyn Error>) -> Self {
        RayexecError {
            msg: msg.into(),
            source: Some(source),
            backtrace: Backtrace::capture(),
        }
    }
}

impl fmt::Display for RayexecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)?;
        if let Some(source) = &self.source {
            write!(f, "Error source: {}", source)?;
        }

        if matches!(self.backtrace.status(), BacktraceStatus::Captured) {
            write!(f, "Backtrace:\n{}", self.backtrace)?;
        }

        Ok(())
    }
}

impl Error for RayexecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref())
    }
}
