use tonic::metadata::AsciiMetadataValue;
use tracing::warn;

pub const RESOLVE_ERROR_STRATEGY_META: &str = "resolve-error-strategy";

/// Additional metadata to provide a hint to the client on what it can do to
/// automatically resolve an error.
///
/// These are only suggestions, and correctness should not depend on the action
/// of the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveErrorStrategy {
    /// Client may refetch the latest catalog and retry the operation.
    FetchCatalogAndRetry,

    /// No known steps to resolve the error. Client should bubble the error up.
    Unknown,
}

impl ResolveErrorStrategy {
    pub fn to_metadata_value(&self) -> AsciiMetadataValue {
        match self {
            Self::Unknown => AsciiMetadataValue::from_static("0"),
            Self::FetchCatalogAndRetry => AsciiMetadataValue::from_static("1"),
        }
    }

    pub fn from_bytes(bs: &[u8]) -> ResolveErrorStrategy {
        match bs.len() {
            1 => match bs[0] {
                b'0' => Self::Unknown,
                b'1' => Self::FetchCatalogAndRetry,
                _ => Self::parse_err(bs),
            },
            _ => Self::parse_err(bs),
        }
    }

    fn parse_err(bs: &[u8]) -> ResolveErrorStrategy {
        warn!(?bs, "failed getting resolve strategy from bytes");
        ResolveErrorStrategy::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_roundtrip() {
        let variants = [
            ResolveErrorStrategy::FetchCatalogAndRetry,
            ResolveErrorStrategy::Unknown,
        ];

        for strat in variants {
            let val = strat.to_metadata_value();
            let out = ResolveErrorStrategy::from_bytes(val.as_bytes());
            assert_eq!(strat, out);
        }
    }
}
