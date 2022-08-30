use crate::page::{DiskPtr, PageId};

#[derive(Debug, thiserror::Error)]
pub enum LlamaError {
    #[error("encountered unknown page kind from disk: {0:?}")]
    UnknownPageKind(DiskPtr),

    #[error("missing page pointer for pid: {0:?}")]
    MissingPagePtr(PageId),

    /// An attempt at updating or replacing a page failed from the ptr being out
    /// of date. Normally the caller should retry.
    #[error("page pointer out of date")]
    PagePtrOutOfDate,

    #[error("channel broken")]
    BrokenChannel,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = LlamaError> = std::result::Result<T, E>;

impl LlamaError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, LlamaError::PagePtrOutOfDate)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for LlamaError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::BrokenChannel
    }
}
