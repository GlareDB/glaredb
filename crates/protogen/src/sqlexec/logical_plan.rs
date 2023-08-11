use datafusion_proto::protobuf::{DfSchema, LogicalPlanNode, OwnedTableReference};
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct CreateTable {
    #[prost(message, optional, tag = "1")]
    pub table_name: Option<OwnedTableReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, optional, tag = "3")]
    pub schema: Option<DfSchema>,
    #[prost(message, optional, tag = "4")]
    pub source: Option<LogicalPlanNode>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Message)]
pub struct LogicalPlanExtension {
    #[prost(oneof = "LogicalPlanExtensionType", tags = "1")]
    pub inner: Option<LogicalPlanExtensionType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Oneof)]
pub enum LogicalPlanExtensionType {
    #[prost(message, tag = "1")]
    DdlPlan(DdlPlanNode),
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Message)]
pub struct DdlPlanNode {
    #[prost(oneof = "DdlPlanType", tags = "1")]
    pub ddl: Option<DdlPlanType>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Oneof)]
pub enum DdlPlanType {
    #[prost(message, tag = "1")]
    CreateTable(CreateTable),
}
