use protogen::metastore::strategy::{ResolveErrorStrategy, RESOLVE_ERROR_STRATEGY_META};
use protogen::metastore::types::catalog::CatalogEntry;

#[derive(thiserror::Error, Debug)]
pub enum MetastoreError {
    #[error("Catalog version mismatch; have: {have}, need: {need}")]
    VersionMismatch { have: u64, need: u64 },

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
    BuiltinObjectPersisted(protogen::metastore::types::catalog::EntryMeta),

    #[error("OID repeated: OID: {oid}; ent1: {ent1:?}, ent2: {ent2:?}")]
    BuiltinRepeatedOid {
        oid: u32,
        ent1: CatalogEntry,
        ent2: CatalogEntry,
    },

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

    #[error("Tunnel '{tunnel} not supported for {action}'")]
    TunnelNotSupportedForAction {
        tunnel: String,
        action: &'static str,
    },

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
    CannotModifyBuiltin(protogen::metastore::types::catalog::CatalogEntry),

    #[error("Cannot exceed {max} objects in a database")]
    MaxNumberOfObjects { max: usize },

    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),

    #[error(transparent)]
    ProtoConv(#[from] protogen::errors::ProtoConvError),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    Validation(#[from] sqlbuiltins::validation::ValidationError),

    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("Cannot specify both 'IF NOT EXISTS' and 'OR REPLACE'")]
    InvalidCreatePolicy,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = MetastoreError> = std::result::Result<T, E>;

impl From<MetastoreError> for tonic::Status {
    fn from(value: MetastoreError) -> Self {
        // Send a strategy to the client, possibly allowing it to resolve the
        // error itself without the user being notified.
        let strat = match &value {
            MetastoreError::VersionMismatch { .. } => ResolveErrorStrategy::FetchCatalogAndRetry,
            _ => ResolveErrorStrategy::Unknown,
        };

        let mut status = tonic::Status::from_error(Box::new(value));
        status
            .metadata_mut()
            .insert(RESOLVE_ERROR_STRATEGY_META, strat.to_metadata_value());
        status
    }
}
