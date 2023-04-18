use crate::errors::{MetastoreError, Result};

// validate object name
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
