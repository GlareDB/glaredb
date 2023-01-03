#[derive(Debug, thiserror::Error)]
pub enum PgReprError {
    #[error("Invalid format code: {0}")]
    InvalidFormatCode(i16),
}

pub type Result<T, E = PgReprError> = std::result::Result<T, E>;
