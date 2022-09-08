use serde::{Deserialize, Serialize};

use crate::rpc::pb::GetSchemaRequest;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataSourceRequest {
    Begin,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataSourceResponse {
    Begin(u64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadTxRequest {
    GetSchema(GetSchemaRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadTxResponse {
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteTxRequest {
    Commit,
    Rollback,
    AllocateTable,
    DeallocateTable,
    Insert,
    AllocateTableIfNotExists,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteTxResponse {
    None,
    TableCreated,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestData {
    DataSource(DataSourceRequest),
    WriteTx(WriteTxRequest),
}

pub use crate::rpc::pb::BinaryWriteRequest as Request;
pub use crate::rpc::pb::BinaryWriteResponse as Response;

impl From<DataSourceRequest> for RequestData {
    fn from(req: DataSourceRequest) -> Self {
        RequestData::DataSource(req)
    }
}

impl From<WriteTxRequest> for RequestData {
    fn from(req: WriteTxRequest) -> Self {
        RequestData::WriteTx(req)
    }
}

impl From<RequestData> for Request {
    fn from(req: RequestData) -> Self {
        Self {
            payload: bincode::serialize(&req).unwrap(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponseData {
    None,
    DataSource(DataSourceResponse),
    WriteTx(WriteTxResponse),
}
