pub mod app;
pub mod client;
pub mod error;
pub mod management;
pub mod messaging;
pub mod network;
pub mod node;
pub mod rpc;
pub mod openraft_types;
pub mod repr;
pub mod store;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::Future;
    use openraft::{testing::StoreBuilder, StorageError};
    use tempdir::TempDir;

    use crate::repr::{NodeId, RaftTypeConfig};
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
