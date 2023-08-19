use prost::Message;

use super::common::PostgresAccess;

#[derive(Clone, PartialEq, Message)]
pub struct PostgresTableProviderConfig {
    #[prost(message, tag = "1")]
    pub access: Option<PostgresAccess>,
    #[prost(string, tag = "2")]
    pub schema: String,
    #[prost(string, tag = "3")]
    pub table: String,
}
