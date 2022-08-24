use std::sync::Arc;

use messaging::{GlareRequest, GlareResponse};
use network::ConsensusNetwork;
use openraft::{BasicNode, Raft};

use self::store::ConsensusStore;

pub mod app;
pub mod client;
pub mod error;
pub mod messaging;
pub mod network;
pub mod node;
pub mod raft;
pub mod store;

pub type GlareNodeId = u64;

pub type GlareNode = BasicNode;
/*
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default, PartialOrd, Ord, Hash)]
pub struct GlareNode {
    pub rpc_addr: String,
}

impl std::fmt::Display for GlareNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "GlareNode {{ }}")
    }
}
*/

pub type GlareRaft = Raft<GlareTypeConfig, Arc<ConsensusNetwork>, Arc<ConsensusStore>>;

openraft::declare_raft_types!(
    pub GlareTypeConfig: D = GlareRequest, R = GlareResponse, NodeId = GlareNodeId, Node = GlareNode
);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::Future;
    use openraft::{testing::StoreBuilder, StorageError};

    use super::{store::ConsensusStore, GlareNodeId, GlareTypeConfig};

    struct ConsensusBuilder {}

    #[async_trait]
    impl StoreBuilder<GlareTypeConfig, Arc<ConsensusStore>> for ConsensusBuilder {
        async fn run_test<Fun, Ret, Res>(&self, t: Fun) -> Result<Ret, StorageError<GlareNodeId>>
        where
            Res: Future<Output = Result<Ret, StorageError<GlareNodeId>>> + Send,
            Fun: Fn(Arc<ConsensusStore>) -> Res + Sync + Send,
        {
            let store = ConsensusStore::new("test2").await;
            t(store).await
        }
    }

    #[test]
    pub fn test_store() {
        openraft::testing::Suite::test_all(ConsensusBuilder {}).expect("failed");
    }
}
