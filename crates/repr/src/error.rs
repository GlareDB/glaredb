#[derive(Debug, thiserror::Error)]
pub enum ReprError {
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
}

pub type Result<T, E = ReprError> = std::result::Result<T, E>;
