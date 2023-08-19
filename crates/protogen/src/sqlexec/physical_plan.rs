mod postgres;
pub use postgres::*;

use datafusion_proto::protobuf::Schema;
use prost::{Message, Oneof};

use super::common::PostgresAccess;

#[derive(Clone, PartialEq, Message)]
pub struct ClientExchangeRecvExec {
    #[prost(bytes, tag = "1")]
    pub broadcast_id: Vec<u8>, // UUID
    #[prost(message, tag = "2")]
    pub schema: Option<Schema>,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum ExecutionPlanExtensionType {
    // DDLs
    #[prost(message, tag = "1")]
    ClientExchangeRecvExec(ClientExchangeRecvExec),
}
