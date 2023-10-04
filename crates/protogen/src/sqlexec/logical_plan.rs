mod copy_to;
use crate::{
    gen::metastore::{
        options::TableOptions,
        service::{
            AlterDatabaseRename, AlterTunnelRotateKeys, CreateCredentials, CreateExternalDatabase,
            CreateTunnel,
        },
    },
    sqlexec::common::{FullObjectReference, FullSchemaReference},
    ProtoConvError,
};

pub use copy_to::*;
use datafusion_proto::protobuf::{DfSchema, LogicalPlanNode};
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct DropCredentials {
    #[prost(string, repeated, tag = "1")]
    pub names: Vec<String>, // TODO: Do these live in schemas?
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
}
#[derive(Clone, PartialEq, Message)]
pub struct DropDatabase {
    #[prost(string, repeated, tag = "1")]
    pub names: Vec<String>,
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropSchemas {
    #[prost(message, repeated, tag = "1")]
    pub references: Vec<FullSchemaReference>,
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
    #[prost(bool, tag = "3")]
    pub cascade: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropTunnel {
    #[prost(string, repeated, tag = "1")]
    pub names: Vec<String>, // TODO: Do these live in schemas?
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateTable {
    #[prost(message, tag = "1")]
    pub reference: Option<FullObjectReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(bool, tag = "3")]
    pub or_replace: bool,
    #[prost(message, optional, tag = "4")]
    pub schema: Option<DfSchema>,
    #[prost(message, optional, tag = "5")]
    pub source: Option<LogicalPlanNode>,
}
#[derive(Clone, PartialEq, Message)]
pub struct CreateTempTable {
    #[prost(message, tag = "1")]
    pub reference: Option<FullObjectReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, tag = "3")]
    pub schema: Option<DfSchema>,
    #[prost(message, optional, tag = "4")]
    pub source: Option<LogicalPlanNode>,
    #[prost(bool, tag = "5")]
    pub or_replace: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateExternalTable {
    #[prost(message, tag = "1")]
    pub reference: Option<FullObjectReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, tag = "3")]
    pub table_options: Option<TableOptions>,
    #[prost(message, optional, tag = "4")]
    pub tunnel: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateView {
    #[prost(message, tag = "1")]
    pub reference: Option<FullObjectReference>,
    #[prost(string, tag = "2")]
    pub sql: String,
    #[prost(message, repeated, tag = "3")]
    pub columns: Vec<String>,
    #[prost(bool, tag = "4")]
    pub or_replace: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateSchema {
    #[prost(message, tag = "1")]
    pub reference: Option<FullSchemaReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropTables {
    #[prost(message, repeated, tag = "1")]
    pub references: Vec<FullObjectReference>,
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropViews {
    #[prost(message, repeated, tag = "1")]
    pub references: Vec<FullObjectReference>,
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct AlterTableRename {
    #[prost(message, tag = "1")]
    pub reference: Option<FullObjectReference>,
    #[prost(message, tag = "2")]
    pub new_reference: Option<FullObjectReference>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetVariable {
    #[prost(string, tag = "1")]
    pub variable: String,
    #[prost(string, tag = "2")]
    pub values: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShowVariable {
    #[prost(string, tag = "1")]
    pub variable: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Update {}

#[derive(Clone, PartialEq, Message)]
pub struct Delete {}

#[derive(Clone, PartialEq, Message)]
pub struct Insert {}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Message)]
pub struct LogicalPlanExtension {
    #[prost(
        oneof = "LogicalPlanExtensionType",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19"
    )]
    pub inner: Option<LogicalPlanExtensionType>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Oneof)]
pub enum LogicalPlanExtensionType {
    // DDLs
    #[prost(message, tag = "1")]
    CreateTable(CreateTable),
    #[prost(message, tag = "2")]
    CreateSchema(CreateSchema),
    #[prost(message, tag = "3")]
    CreateExternalTable(CreateExternalTable),
    #[prost(message, tag = "4")]
    DropTables(DropTables),
    #[prost(message, tag = "5")]
    AlterTableRename(AlterTableRename),
    #[prost(message, tag = "6")]
    AlterDatabaseRename(AlterDatabaseRename),
    #[prost(message, tag = "7")]
    AlterTunnelRotateKeys(AlterTunnelRotateKeys),
    #[prost(message, tag = "8")]
    CreateCredentials(CreateCredentials),
    #[prost(message, tag = "9")]
    CreateExternalDatabase(CreateExternalDatabase),
    #[prost(message, tag = "10")]
    CreateTunnel(CreateTunnel),
    #[prost(message, tag = "11")]
    CreateTempTable(CreateTempTable),
    #[prost(message, tag = "12")]
    CreateView(CreateView),
    #[prost(message, tag = "13")]
    DropCredentials(DropCredentials),
    #[prost(message, tag = "14")]
    DropDatabase(DropDatabase),
    #[prost(message, tag = "15")]
    DropSchemas(DropSchemas),
    #[prost(message, tag = "16")]
    DropTunnel(DropTunnel),
    #[prost(message, tag = "17")]
    DropViews(DropViews),
    #[prost(message, tag = "18")]
    SetVariable(SetVariable),
    #[prost(message, tag = "19")]
    CopyTo(CopyTo),
}
