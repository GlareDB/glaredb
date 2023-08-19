use datafusion_proto::protobuf::Schema;
use prost::Message;

use super::common::PostgresAccess;

#[derive(Clone, PartialEq, Message)]
pub struct ClientExchangeRecvExec {
    #[prost(bytes, tag = "1")]
    pub broadcast_id: Vec<u8>, // UUID
    #[prost(message, tag = "2")]
    pub schema: Option<Schema>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PostgresBinaryCopyConfig {
    #[prost(message, tag = "1")]
    pub access: Option<PostgresAccess>,
    #[prost(string, tag = "2")]
    pub schema: String,
    #[prost(string, tag = "3")]
    pub table: String,
    #[prost(string, tag = "4")]
    pub copy_query: String,
}
