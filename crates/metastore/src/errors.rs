use tonic::metadata::AsciiMetadataValue;
use tracing::warn;

#[derive(thiserror::Error, Debug)]
pub enum MetastoreError {
    #[error("Duplicate name: {0}")]
    DuplicateName(String),

    #[error("Invalid object name length: {length}, max: {max}")]
    InvalidNameLength { length: usize, max: usize },

    #[error("Duplicate object names in the '{object_namespace}' namespace found during load; name {name}, schema: {schema}, first: {first}, second: {second}")]
    DuplicateNameFoundDuringLoad {
        name: String,
        schema: u32,
        first: u32,
        second: u32,
        object_namespace: &'static str,
    },

    #[error("Builtin object persisted when it shouldn't have been: {0:?}")]
    BuiltinObjectPersisted(metastoreproto::types::catalog::EntryMeta),

    #[error("Missing database catalog: {0}")]
    MissingCatalog(uuid::Uuid),

    #[error("Missing database: {0}")]
    MissingDatabase(String),

    #[error("Missing tunnel: {0}")]
    MissingTunnel(String),

    #[error("Missing credentials: {0}")]
    MissingCredentials(String),

    #[error("Missing schema: {0}")]
    MissingNamedSchema(String),

    #[error("Missing database object; schema: {schema}, name: {name}")]
    MissingNamedObject { schema: String, name: String },

    #[error("Missing entry: {0}")]
    MissingEntry(u32),

    #[error("Tunnel '{tunnel}' not supported by datasource '{datasource}'")]
    TunnelNotSupportedByDatasource { tunnel: String, datasource: String },

    #[error("Credentials '{credentials}' not supported by datasource '{datasource}'")]
    CredentialsNotSupportedByDatasource {
        credentials: String,
        datasource: String,
    },

    #[error("Tunnel '{tunnel} not supported for {action}'")]
    TunnelNotSupportedForAction {
        tunnel: String,
        action: &'static str,
    },

    #[error("Catalog version mismatch; have: {have}, need: {need}")]
    VersionMismtatch { have: u64, need: u64 },

    #[error("Invalid database id: {0:?}")]
    InvalidDatabaseId(Vec<u8>),

    #[error("Object {object} of type '{object_type}' has non-zero parent: {parent}")]
    ObjectHasNonZeroParent {
        object: u32,
        parent: u32,
        object_type: &'static str,
    },

    #[error("Schema {schema} has {num_objects} child objects")]
    SchemaHasChildren { schema: u32, num_objects: usize },

    #[error("Object {object} of type '{object_type}' has invalid parent id: {parent}")]
    ObjectHasInvalidParentId {
        object: u32,
        parent: u32,
        object_type: &'static str,
    },

    #[error("Failed in-process startup: {0}")]
    FailedInProcessStartup(String),

    #[error("Cannot modify builtin object: {0:?}")]
    CannotModifyBuiltin(metastoreproto::types::catalog::CatalogEntry),

    #[error("Cannot exceed {max} objects in a database")]
    MaxNumberOfObjects { max: usize },

    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),

    #[error(transparent)]
    ProtoConv(#[from] metastoreproto::types::ProtoConvError),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error("Feature unimplemented: {0}")]
    Unimplemented(&'static str),
}

pub type Result<T, E = MetastoreError> = std::result::Result<T, E>;

impl From<MetastoreError> for tonic::Status {
    fn from(value: MetastoreError) -> Self {
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

impl MetastoreError {
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
