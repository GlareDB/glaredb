//! Distributed storage engine.
mod accord;
pub mod client;
pub mod store;
pub mod stream;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("internal: {0}")]
    Internal(String),
    #[error(transparent)]
    CoretypesColumn(#[from] coretypes::column::ColumnError),
    #[error(transparent)]
    CoretypesBatch(#[from] coretypes::batch::BatchError),
    #[error(transparent)]
    CoretypesExpr(#[from] coretypes::expr::ExprError),
}

pub type Result<T, E = StoreError> = std::result::Result<T, E>;
