#[derive(Debug, thiserror::Error)]
pub enum LlamaError {
    /// An attempt at updating or replacing a page failed from the ptr being out
    /// of date. Normally the caller should retry.
    #[error("page pointer out of date")]
    PagePtrOutOfData,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = LlamaError> = std::result::Result<T, E>;

impl LlamaError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, LlamaError::PagePtrOutOfData)
    }
}
