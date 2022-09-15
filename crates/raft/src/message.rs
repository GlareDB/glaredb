use serde::{Deserialize, Serialize};

use crate::rpc::pb::{BinaryReadRequest, BinaryReadResponse, BinaryWriteRequest};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadTxRequest {
    Placeholder,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadTxResponse {
    Placeholder,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteTxRequest {
    Placeholder,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteTxResponse {
    None,
    TableCreated,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    WriteTx(WriteTxRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    None,
}

impl From<WriteTxRequest> for Request {
    fn from(req: WriteTxRequest) -> Self {
        Request::WriteTx(req)
    }
}

impl From<Request> for BinaryWriteRequest {
    fn from(req: Request) -> Self {
        Self {
            payload: bincode::serialize(&req).unwrap(),
        }
    }
}

impl From<ReadTxRequest> for BinaryReadRequest {
    fn from(req: ReadTxRequest) -> Self {
        Self {
            payload: bincode::serialize(&req).unwrap(),
        }
    }
}

impl From<BinaryReadResponse> for ReadTxResponse {
    fn from(resp: BinaryReadResponse) -> Self {
        bincode::deserialize(&resp.payload).unwrap()
    }
}
