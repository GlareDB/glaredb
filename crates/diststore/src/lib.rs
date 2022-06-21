//! Distributed storage engine.
mod accord;
pub mod client;
pub mod store;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("internal: {0}")]
    Internal(String),
    #[error(transparent)]
    CoretypesColumn(#[from] coretypes::column::ColumnError),
}

pub type Result<T, E = StoreError> = std::result::Result<T, E>;
