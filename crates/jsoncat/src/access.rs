//! Access methods.
use serde::{Deserialize, Serialize};
use std::fmt;

/// How we access tables.
#[derive(Debug, Serialize, Deserialize)]
pub enum AccessMethod {
    Unknown,
    InternalMemory,
}

impl fmt::Display for AccessMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccessMethod::Unknown => write!(f, "unknown"),
            AccessMethod::InternalMemory => write!(f, "internal memory"),
        }
    }
}
