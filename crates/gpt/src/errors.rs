#[derive(Debug, thiserror::Error)]
pub enum GptError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = GptError> = std::result::Result<T, E>;
