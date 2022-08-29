#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("missing transaction for wal replay: {0}")]
    MissingTxForReplay(u64),

    #[error("channel broken")]
    BrokenChannel,

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub type Result<T, E = WalError> = std::result::Result<T, E>;
