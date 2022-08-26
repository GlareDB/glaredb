pub mod app;
pub mod client;
pub mod error;
pub mod message;
pub mod network;
pub mod openraft_types;
pub mod repr;
pub mod server;
pub mod store;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::Future;
    use openraft::testing::StoreBuilder;
    use tempdir::TempDir;

    use crate::{repr::{RaftTypeConfig}, openraft_types::types::StorageError};
    use super::store::ConsensusStore;

    struct ConsensusBuilder;

    #[async_trait]
    impl StoreBuilder<RaftTypeConfig, Arc<ConsensusStore>> for ConsensusBuilder {
        async fn run_test<Fun, Ret, Res>(&self, t: Fun) -> Result<Ret, StorageError>
        where
            Res: Future<Output = Result<Ret, StorageError>> + Send,
            Fun: Fn(Arc<ConsensusStore>) -> Res + Sync + Send,
        {
            let temp = TempDir::new("consensus").unwrap();
            let store = ConsensusStore::new(&temp).await;
            t(store).await
        }
    }

    #[test]
    pub fn test_store() {
        openraft::testing::Suite::test_all(ConsensusBuilder {}).expect("failed");
    }
}
