#[derive(Debug, thiserror::Error)]
pub enum GptError {
    #[error("GPT client not configured.")]
    GptClientNotConfigured,

    #[error("No completion results.")]
    NoCompletions,

    #[error(transparent)]
    Gpt(#[from] gpt::errors::GptError),
}

pub type Result<T, E = GptError> = std::result::Result<T, E>;
