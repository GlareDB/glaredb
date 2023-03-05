#[derive(Debug, thiserror::Error)]
pub enum MongoError {
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
}

pub type Result<T, E = MongoError> = std::result::Result<T, E>;
