use std::backtrace::{Backtrace, BacktraceStatus};
use std::error::Error;
use std::fmt;

pub type Result<T, E = DbError> = std::result::Result<T, E>;

/// Helper macros for returning an error for currently unimplemented items.
///
/// This should generally be used in place of the `unimplemented` macro.
#[macro_export]
macro_rules! not_implemented {
    ($($arg:tt)+) => {{
        let msg = format!($($arg)+);
        return Err($crate::DbError::new(format!("Not yet implemented: {msg}")));
    }};
}

// TODO: Implement partial eq on msg
pub struct DbError {
    inner: Box<RayexecErrorInner>,
}

struct RayexecErrorInner {
    /// Message for the error.
    pub msg: String,
    /// Source of the error.
    pub source: Option<Box<dyn Error + Send + Sync>>,
    /// Captured backtrace for the error.
    ///
    /// Enable with the RUST_BACKTRACE env var.
    pub backtrace: Backtrace,
    /// Extra error fields to display.
    pub extra_fields: Vec<ErrorField>,
}

pub trait ErrorFieldValue: fmt::Debug + fmt::Display + Sync + Send {}

impl<T: fmt::Debug + fmt::Display + Sync + Send> ErrorFieldValue for T {}

#[derive(Debug)]
pub struct ErrorField {
    pub key: String,
    pub value: Box<dyn ErrorFieldValue>,
}

impl DbError {
    pub fn new(msg: impl Into<String>) -> Self {
        DbError {
            inner: Box::new(RayexecErrorInner {
                msg: msg.into(),
                source: None,
                backtrace: Backtrace::capture(),
                extra_fields: Vec::new(),
            }),
        }
    }

    pub fn with_source(msg: impl Into<String>, source: Box<dyn Error + Send + Sync>) -> Self {
        DbError {
            inner: Box::new(RayexecErrorInner {
                msg: msg.into(),
                source: Some(source),
                backtrace: Backtrace::capture(),
                extra_fields: Vec::new(),
            }),
        }
    }

    pub fn with_field<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: ErrorFieldValue + 'static,
    {
        self.inner.extra_fields.push(ErrorField {
            key: key.into(),
            value: Box::new(value),
        });
        self
    }

    // TODO: dyn
    pub fn with_fields<K, V, I>(mut self, fields: I) -> Self
    where
        K: Into<String>,
        V: ErrorFieldValue + 'static,
        I: IntoIterator<Item = (K, V)>,
    {
        self.inner
            .extra_fields
            .extend(fields.into_iter().map(|(key, value)| ErrorField {
                key: key.into(),
                value: Box::new(value),
            }));
        self
    }

    pub fn get_msg(&self) -> &str {
        self.inner.msg.as_str()
    }

    pub fn get_backtrace(&self) -> &Backtrace {
        &self.inner.backtrace
    }
}

impl From<fmt::Error> for DbError {
    fn from(value: fmt::Error) -> Self {
        Self::with_source("Format error", Box::new(value))
    }
}

impl From<std::io::Error> for DbError {
    fn from(value: std::io::Error) -> Self {
        Self::with_source("IO error", Box::new(value))
    }
}

// TODO: This loses a bit of context surrounding the source of the error. What
// was the value? What were we converting to?
//
// Likely this should be removed once we're in the polishing phase.
impl From<std::num::TryFromIntError> for DbError {
    fn from(value: std::num::TryFromIntError) -> Self {
        Self::with_source("Int convert error", Box::new(value))
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.msg)?;

        for extra in &self.inner.extra_fields {
            write!(f, "\n{}: {}", extra.key, extra.value)?;
        }

        if let Some(source) = &self.inner.source {
            write!(f, "\nError source: {source}")?;
        }

        if self.inner.backtrace.status() == BacktraceStatus::Captured {
            write!(f, "\nBacktrace: {}", self.inner.backtrace)?
        }

        Ok(())
    }
}

// Debug implemented by using the Display out as that's significantly easier to
// read.
impl fmt::Debug for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Error for DbError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner.source.as_ref().map(|e| e.as_ref() as _)
    }
}

/// An extension trait for adding context to the Error variant of a result.
pub trait ResultExt<T, E> {
    /// Wrap an error with a static context string.
    fn context(self, msg: &'static str) -> Result<T>;

    /// Wrap an error with a context string generated from a function.
    fn context_fn<F: Fn() -> String>(self, f: F) -> Result<T>;
}

impl<T, E: Error + Send + Sync + 'static> ResultExt<T, E> for std::result::Result<T, E> {
    fn context(self, msg: &'static str) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(DbError::with_source(msg, Box::new(e))),
        }
    }

    fn context_fn<F: Fn() -> String>(self, f: F) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(DbError::with_source(f(), Box::new(e))),
        }
    }
}

pub trait OptionExt<T> {
    /// Return an error with the given message if the the Option is None.
    fn required(self, msg: &'static str) -> Result<T>;
}

impl<T> OptionExt<T> for Option<T> {
    fn required(self, msg: &'static str) -> Result<T> {
        match self {
            Self::Some(v) => Ok(v),
            None => Err(DbError::new(msg)),
        }
    }
}
