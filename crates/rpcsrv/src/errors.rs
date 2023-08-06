#[derive(Debug, thiserror::Error)]
pub enum RpcsrvError {
    #[error("Invalid database id: {0:?}")]
    InvalidDatabaseId(Vec<u8>),

    #[error(transparent)]
    ExecError(#[from] sqlexec::errors::ExecError),
}

pub type Result<T, E = RpcsrvError> = std::result::Result<T, E>;

impl From<RpcsrvError> for tonic::Status {
    fn from(value: RpcsrvError) -> Self {
        tonic::Status::from_error(Box::new(value))
    }
}
