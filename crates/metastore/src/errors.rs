#[derive(thiserror::Error, Debug)]
pub enum MetastoreError {
    #[error("Duplicate name: {0}")]
    DuplicateName(String),

    #[error("Missing database catalog: {0}")]
    MissingCatalog(uuid::Uuid),

    #[error("Missing schema: {0}")]
    MissingNamedSchema(String),

    #[error("Missing database object; schema: {schema}, name: {name}")]
    MissingNamedObject { schema: String, name: String },

    #[error("Missing entry: {0}")]
    MissingEntry(u32),

    #[error("Catalog version mismatch; have: {have}, need: {need}")]
    VersionMismtatch { have: u64, need: u64 },

    #[error("Invalid database id: {0:?}")]
    InvalidDatabaseId(Vec<u8>),

    #[error(transparent)]
    ProtoConv(#[from] crate::types::ProtoConvError),
}

pub type Result<T, E = MetastoreError> = std::result::Result<T, E>;

impl From<MetastoreError> for tonic::Status {
    fn from(value: MetastoreError) -> Self {
        match value {
            other => tonic::Status::from_error(Box::new(other)),
        }
    }
}
