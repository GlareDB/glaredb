use crate::ProtoConvError;
use std::borrow::Cow;

use datafusion_proto::protobuf::{DfSchema, LogicalPlanNode, OwnedTableReference};
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct ClientExchangeRecvExec {
    #[prost(bytes, tag = "1")]
    pub broadcast_id: Vec<u8>, // UUID
    #[prost(message, tag = "2")]
    pub schema: Option<DfSchema>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Oneof)]
pub enum PhysicalPlanExtensionType {
    #[prost(message, tag = "1")]
    ClientExchangeRecvExec(ClientExchangeRecvExec),
}
