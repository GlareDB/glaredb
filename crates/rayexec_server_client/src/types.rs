use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageOwned {
    pub api_version: usize,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message<'a> {
    pub api_version: usize,
    #[serde(with = "serde_bytes")]
    pub payload: &'a [u8],
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanAstRpcRequest {
    // TODO: Unsure if we want to keep this opaque at this level or not. If
    // don't then it's likely that well need to move these types elsewhere to
    // avoid a dependency cycle.
    pub ast_data: Vec<u8>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanAstRpcResponse {
    pub hello: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PushBatchRcpRequest {
    // TODO: id
    pub ipc_data: Vec<u8>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PushBatchRpcResponse {}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PullBatchRpcRequest {
    // TODO: id
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PullBatchRpcResponse {
    pub ipc_data: Vec<u8>,
}
