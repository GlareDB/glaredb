mod postgres;
pub use postgres::*;

use datafusion_proto::protobuf::{LogicalExprNode, Schema};
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct ClientExchangeRecvExec {
    #[prost(bytes, tag = "1")]
    pub broadcast_id: Vec<u8>, // UUID
    #[prost(message, tag = "2")]
    pub schema: Option<Schema>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RemoteScanExec {
    #[prost(bytes, tag = "1")]
    pub provider_id: Vec<u8>, // UUID
    #[prost(message, tag = "2")]
    pub projected_schema: Option<Schema>,
    #[prost(uint64, repeated, tag = "3")]
    pub projection: Vec<u64>,
    #[prost(message, repeated, tag = "4")]
    pub filters: Vec<LogicalExprNode>,
    #[prost(uint64, optional, tag = "5")]
    pub limit: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AlterDatabaseRenameExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub new_name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct AlterTableRenameExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub new_name: String,
    #[prost(string, tag = "4")]
    pub schema: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecutionPlanExtension {
    #[prost(oneof = "ExecutionPlanExtensionType", tags = "1, 2, 3, 4")]
    pub inner: Option<ExecutionPlanExtensionType>,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum ExecutionPlanExtensionType {
    // Exchanges
    #[prost(message, tag = "1")]
    ClientExchangeRecvExec(ClientExchangeRecvExec),
    // Scans
    #[prost(message, tag = "2")]
    RemoteScanExec(RemoteScanExec),
    // DDLs
    #[prost(message, tag = "3")]
    AlterDatabaseRenameExec(AlterDatabaseRenameExec),
    #[prost(message, tag = "4")]
    AlterTableRenameExec(AlterTableRenameExec),
}
