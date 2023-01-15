#[derive(thiserror::Error, Debug)]
pub enum MetastoreError {
    #[error(transparent)]
    ProtoConv(#[from] crate::types::ProtoConvError),
}

pub type Result<T, E = MetastoreError> = std::result::Result<T, E>;
