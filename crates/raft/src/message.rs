use lemur::repr::{
    df::{DataFrame, Schema},
    expr::ScalarExpr,
    relation::RelationKey,
};
use serde::{Deserialize, Serialize};

use crate::rpc::pb::{BinaryReadRequest, BinaryReadResponse, BinaryWriteRequest, GetSchemaRequest};

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
    Insert(InsertRequest),
    AllocateTableIfNotExists,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InsertRequest {
    pub table: RelationKey,
    pub data: DataFrame,
    pub pk_idxs: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteTxResponse {
    None,
    TableCreated,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    DataSource(DataSourceRequest),
    WriteTx(WriteTxRequest),
    // ReadTx(ReadTxRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    None,
    DataSource(DataSourceResponse),
    WriteTx(WriteTxResponse),
    // ReadTx(ReadTxResponse),
}

impl From<DataSourceRequest> for Request {
    fn from(req: DataSourceRequest) -> Self {
        Request::DataSource(req)
    }
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
