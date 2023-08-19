use prost::Message;

use crate::sqlexec::common::PostgresAccess;

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
