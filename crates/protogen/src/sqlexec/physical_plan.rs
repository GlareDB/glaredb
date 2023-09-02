mod postgres;
pub use postgres::*;

use crate::gen::metastore::catalog::TableEntry;
use datafusion_proto::protobuf::{LogicalExprNode, PhysicalExprNode, Schema};
use prost::{Enumeration, Message, Oneof};

use super::{
    common::{FullObjectReference, FullSchemaReference},
    logical_plan::{CopyToDestinationOptions, CopyToFormatOptions},
};

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
pub struct CreateTableExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(message, tag = "2")]
    pub tbl_reference: Option<FullObjectReference>,
    #[prost(bool, tag = "3")]
    pub if_not_exists: bool,
    #[prost(message, tag = "4")]
    pub arrow_schema: Option<Schema>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateCredentialsExec {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(uint64, tag = "2")]
    pub catalog_version: u64,
    #[prost(message, tag = "3")]
    pub options: Option<crate::gen::metastore::options::CredentialsOptions>,
    #[prost(string, tag = "4")]
    pub comment: String,
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
    #[prost(message, tag = "2")]
    pub tbl_reference: Option<FullObjectReference>,
    #[prost(message, tag = "3")]
    pub new_tbl_reference: Option<FullObjectReference>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AlterTunnelRotateKeysExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(bool, tag = "3")]
    pub if_exists: bool,
    #[prost(bytes = "vec", tag = "4")]
    pub new_ssh_key: ::prost::alloc::vec::Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropDatabaseExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, repeated, tag = "2")]
    pub names: Vec<String>,
    #[prost(bool, tag = "3")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropSchemasExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(message, repeated, tag = "2")]
    pub schema_references: Vec<FullSchemaReference>,
    #[prost(bool, tag = "3")]
    pub if_exists: bool,
    #[prost(bool, tag = "4")]
    pub cascade: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropTunnelExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, repeated, tag = "2")]
    pub names: Vec<String>,
    #[prost(bool, tag = "3")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropViewsExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(message, repeated, tag = "2")]
    pub view_references: Vec<FullObjectReference>,
    #[prost(bool, tag = "3")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateSchema {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(message, tag = "2")]
    pub schema_reference: Option<FullSchemaReference>,
    #[prost(bool, tag = "3")]
    pub if_not_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateTempTableExec {
    #[prost(message, tag = "1")]
    pub tbl_reference: Option<FullObjectReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, tag = "3")]
    pub arrow_schema: Option<Schema>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateExternalDatabaseExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, tag = "2")]
    pub database_name: String,
    #[prost(message, tag = "3")]
    pub options: Option<crate::gen::metastore::options::DatabaseOptions>,
    #[prost(bool, tag = "4")]
    pub if_not_exists: bool,
    #[prost(string, optional, tag = "5")]
    pub tunnel: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateExternalTableExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(message, tag = "2")]
    pub tbl_reference: Option<FullObjectReference>,
    #[prost(bool, tag = "3")]
    pub if_not_exists: bool,
    #[prost(message, tag = "4")]
    pub table_options: Option<crate::gen::metastore::options::TableOptions>,
    #[prost(message, optional, tag = "5")]
    pub tunnel: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateTunnelExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(message, tag = "3")]
    pub options: Option<crate::gen::metastore::options::TunnelOptions>,
    #[prost(bool, tag = "4")]
    pub if_not_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateViewExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(message, tag = "2")]
    pub view_reference: Option<FullObjectReference>,
    #[prost(string, tag = "3")]
    pub sql: String,
    #[prost(message, repeated, tag = "4")]
    pub columns: Vec<String>,
    #[prost(bool, tag = "5")]
    pub or_replace: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropCredentialsExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(string, repeated, tag = "2")]
    pub names: Vec<String>, // TODO: Do these live in schemas?
    #[prost(bool, tag = "3")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropTablesExec {
    #[prost(uint64, tag = "1")]
    pub catalog_version: u64,
    #[prost(message, repeated, tag = "2")]
    pub tbl_references: Vec<FullObjectReference>,
    #[prost(bool, tag = "3")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetVarExec {
    #[prost(string, tag = "1")]
    pub variable: String,
    #[prost(string, tag = "2")]
    pub values: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShowVarExec {
    #[prost(string, tag = "1")]
    pub variable: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct UpdateSelector {
    #[prost(string, tag = "1")]
    pub column: String,
    #[prost(message, tag = "2")]
    pub expr: Option<LogicalExprNode>,
}

#[derive(Clone, PartialEq, Message)]
pub struct UpdateExec {
    #[prost(message, tag = "1")]
    pub table: Option<TableEntry>,
    #[prost(message, repeated, tag = "2")]
    pub updates: Vec<UpdateSelector>,
    #[prost(message, optional, tag = "3")]
    pub where_expr: Option<LogicalExprNode>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteExec {
    #[prost(message, tag = "1")]
    pub table: Option<TableEntry>,
    #[prost(message, optional, tag = "2")]
    pub where_expr: Option<LogicalExprNode>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InsertExec {
    #[prost(message, tag = "1")]
    pub table: Option<TableEntry>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToExec {
    #[prost(message, tag = "1")]
    pub format: Option<CopyToFormatOptions>,
    #[prost(message, tag = "2")]
    pub dest: Option<CopyToDestinationOptions>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ValuesExec {
    #[prost(message, tag = "1")]
    pub schema: Option<Schema>,
    #[prost(bytes, tag = "2")]
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InterleaveExec {}

#[derive(Clone, PartialEq, Debug, Enumeration)]
#[repr(i32)]
pub enum PartitionSearchMode {
    Linear = 1,
    PartiallySorted = 2,
    Sorted = 3,
}

#[derive(Clone, PartialEq, Message)]
pub struct BoundedWindowAggExec {
    #[prost(message, repeated, tag = "1")]
    pub window_expr: Vec<PhysicalExprNode>,
    #[prost(string, repeated, tag = "2")]
    pub window_expr_name: Vec<String>,
    #[prost(message, tag = "3")]
    pub input_schema: Option<Schema>,
    #[prost(message, repeated, tag = "4")]
    pub partition_keys: Vec<PhysicalExprNode>,
    #[prost(enumeration = "PartitionSearchMode", tag = "5")]
    pub partition_search_mode: i32,
    #[prost(uint64, repeated, tag = "6")]
    pub sorted_columns: Vec<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecutionPlanExtension {
    #[prost(
        oneof = "ExecutionPlanExtensionType",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28"
    )]
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
    #[prost(message, tag = "5")]
    CreateCredentialsExec(CreateCredentialsExec),
    #[prost(message, tag = "6")]
    AlterTunnelRotateKeysExec(AlterTunnelRotateKeysExec),
    #[prost(message, tag = "7")]
    DropDatabaseExec(DropDatabaseExec),
    #[prost(message, tag = "8")]
    DropSchemasExec(DropSchemasExec),
    #[prost(message, tag = "9")]
    DropTunnelExec(DropTunnelExec),
    #[prost(message, tag = "10")]
    DropViewsExec(DropViewsExec),
    #[prost(message, tag = "11")]
    CreateSchema(CreateSchema),
    #[prost(message, tag = "12")]
    CreateTableExec(CreateTableExec),
    #[prost(message, tag = "13")]
    CreateTempTableExec(CreateTempTableExec),
    #[prost(message, tag = "14")]
    CreateExternalDatabaseExec(CreateExternalDatabaseExec),
    #[prost(message, tag = "15")]
    CreateExternalTableExec(CreateExternalTableExec),
    #[prost(message, tag = "16")]
    CreateTunnelExec(CreateTunnelExec),
    #[prost(message, tag = "17")]
    CreateViewExec(CreateViewExec),
    #[prost(message, tag = "18")]
    DropCredentialsExec(DropCredentialsExec),
    #[prost(message, tag = "19")]
    DropTablesExec(DropTablesExec),
    #[prost(message, tag = "20")]
    SetVarExec(SetVarExec),
    #[prost(message, tag = "21")]
    ShowVarExec(ShowVarExec),
    // DML
    #[prost(message, tag = "22")]
    UpdateExec(UpdateExec),
    #[prost(message, tag = "23")]
    InsertExec(InsertExec),
    #[prost(message, tag = "24")]
    DeleteExec(DeleteExec),
    #[prost(message, tag = "25")]
    CopyToExec(CopyToExec),
    // Other
    #[prost(message, tag = "26")]
    ValuesExec(ValuesExec),
    #[prost(message, tag = "27")]
    InterleaveExec(InterleaveExec),
    #[prost(message, tag = "28")]
    BoundedWindowAggExec(BoundedWindowAggExec),
}
