use std::convert::TryFrom;

use crate::error::{PgReprError, Result};

/// Postgres paramater formats.
#[derive(Debug, Clone, Copy)]
pub enum Format {
    Text,
    Binary,
}

impl From<Format> for i16 {
    fn from(format: Format) -> Self {
        match format {
            Format::Text => 0,
            Format::Binary => 1,
        }
    }
}

impl TryFrom<i16> for Format {
    type Error = PgReprError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Format::Text),
            1 => Ok(Format::Binary),
            other => Err(PgReprError::InvalidFormatCode(other)),
        }
    }
}
