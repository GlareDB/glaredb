use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt::Debug;
use url::Url;

use crate::{
    execution::executable::pipeline::ExecutablePartitionPipeline,
    logical::sql::binder::StatementWithBindData,
};

pub const API_VERSION: usize = 0;

pub const REMOTE_ENDPOINTS: Endpoints = Endpoints {
    healthz: "/healthz",
    rpc_hybrid_run: "/rpc/v0/hybrid/run",
    rpc_hybrid_push: "/rpc/v0/hybrid/push_batch",
    rpc_hybrid_pull: "/rpc/v0/hybrid/pull_batch",
};

#[derive(Debug)]
pub struct Endpoints {
    pub healthz: &'static str,
    pub rpc_hybrid_run: &'static str,
    pub rpc_hybrid_push: &'static str,
    pub rpc_hybrid_pull: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridConnectConfig {
    pub remote: Url,
}

pub trait HybridClient: Debug + Sync + Send {
    fn ping(&self) -> BoxFuture<'_, Result<()>>;

    fn remote_bind(
        &self,
        statement: StatementWithBindData,
    ) -> BoxFuture<'_, Result<Vec<ExecutablePartitionPipeline>>>;

    // TODO: batch enum (more?, done?), query id
    fn pull(&self) -> BoxFuture<'_, Result<Option<Batch>>>;

    // TODO: Query id
    fn push(&self, batch: Batch) -> BoxFuture<'_, Result<()>>;
}
