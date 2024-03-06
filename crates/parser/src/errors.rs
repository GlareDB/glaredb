pub use datafusion::sql::sqlparser::parser::ParserError;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("SQL statement currently unsupported: {0}")]
    UnsupportedSQLStatement(String),
    #[error(transparent)]
    SqlParser(#[from] ParserError),
    #[error("Invalid object name length: {length}, max: {max}")]
    InvalidNameLength { length: usize, max: usize },
    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T, E = ParseError> = std::result::Result<T, E>;
impl From<ParseError> for ParserError {
    fn from(e: ParseError) -> Self {
        ParserError::ParserError(e.to_string())
    }
}
