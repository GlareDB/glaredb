use tonic::metadata::AsciiMetadataValue;
use tracing::warn;

#[derive(thiserror::Error, Debug)]
pub enum MetastoreClientError {
    #[error("Invalid object name length: {length}, max: {max}")]
    InvalidNameLength { length: usize, max: usize },

    #[error("Tunnel '{tunnel}' not supported by datasource '{datasource}'")]
    TunnelNotSupportedByDatasource { tunnel: String, datasource: String },

    #[error("Credentials '{credentials}' not supported by datasource '{datasource}'")]
    CredentialsNotSupportedByDatasource {
        credentials: String,
        datasource: String,
    },

    #[error("Format '{format}' not supported by datasource '{datasource}'")]
    FormatNotSupportedByDatasource { format: String, datasource: String },

    #[error("Catalog version mismatch; have: {have}, need: {need}")]
    VersionMismtatch { have: u64, need: u64 },

    #[error(transparent)]
    ProtoConv(#[from] crate::types::ProtoConvError),
}

pub type Result<T, E = MetastoreClientError> = std::result::Result<T, E>;

impl From<MetastoreClientError> for tonic::Status {
    fn from(value: MetastoreClientError) -> Self {
        let strat = value.resolve_error_strategy();
        let mut status = tonic::Status::from_error(Box::new(value));
        status
            .metadata_mut()
            .insert(RESOLVE_ERROR_STRATEGY_META, strat.to_metadata_value());
        status
    }
}

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

impl MetastoreClientError {
    pub fn resolve_error_strategy(&self) -> ResolveErrorStrategy {
        match self {
            Self::VersionMismtatch { .. } => ResolveErrorStrategy::FetchCatalogAndRetry,
            _ => ResolveErrorStrategy::Unknown,
        }
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
