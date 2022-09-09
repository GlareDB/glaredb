use lemur::repr::{df::{Schema, DataFrame}, relation::RelationKey, expr::ScalarExpr};
use serde::{Deserialize, Serialize};

use crate::rpc::pb::{GetSchemaRequest, BinaryReadRequest, BinaryReadResponse};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataSourceRequest {
    Begin,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataSourceResponse {
    Begin(u64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScanRequest {
    pub table: RelationKey,
    pub filter: Option<ScalarExpr>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadTxRequest {
    GetSchema(GetSchemaRequest),
    Scan(ScanRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadTxResponse {
    TableSchema(Option<Schema>),
    Scan(Option<Vec<DataFrame>>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteTxRequest {
    Commit,
    Rollback,
    AllocateTable(RelationKey, Schema),
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
    // ReadTx(ReadTxRequest),
}

pub use RequestData as Request;
pub use ResponseData as Response;
use crate::rpc::pb::BinaryWriteRequest;
// pub use crate::rpc::pb::BinaryWriteResponse as Response;

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

impl From<RequestData> for BinaryWriteRequest {
    fn from(req: RequestData) -> Self {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponseData {
    None,
    DataSource(DataSourceResponse),
    WriteTx(WriteTxResponse),
    // ReadTx(ReadTxResponse),
}
