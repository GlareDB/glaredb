use crate::errors::{MetastoreError, Result};

/// Validate idents as per postgres identifier
/// syntax](https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)
pub fn validate_object_name(name: &str) -> Result<()> {
    const POSTGRES_IDENT_MAX_LENGTH: usize = 63;
    if name.len() > POSTGRES_IDENT_MAX_LENGTH {
        return Err(MetastoreError::InvalidNameLength {
            length: name.len(),
            max: POSTGRES_IDENT_MAX_LENGTH,
        });
    }

    Ok(())
}
