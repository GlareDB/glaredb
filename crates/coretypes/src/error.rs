/// An internal error for the coretypes crate.
///
/// This is a relatively lazy way to handle errors, and is mostly here to get
/// something going fast.
#[derive(Debug, thiserror::Error)]
#[error("internal: {0}")]
pub struct InternalError(String);

macro_rules! internal {
    ($($arg:tt)*) => {
        $crate::error::InternalError(format!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_sanity() {
        let err = internal!("error: {}", "string");
        let expected = InternalError("error: string".to_owned());
        assert_eq!(expected.to_string(), err.to_string());
    }
}
