use prost::Message;

use crate::gen::metastore::options::TunnelOptions;

// TODO: Move me
#[derive(Clone, PartialEq, Message)]
pub struct PostgresAccess {
    #[prost(string, tag = "1")]
    pub conn_str: String,
    #[prost(message, optional, tag = "2")]
    pub tunnel: Option<TunnelOptions>,
}

#[derive(Clone, PartialEq, Message)]
pub struct FullObjectReference {
    #[prost(string, tag = "1")]
    pub database: String,
    #[prost(string, tag = "2")]
    pub schema: String,
    #[prost(string, tag = "3")]
    pub name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct FullSchemaReference {
    #[prost(string, tag = "1")]
    pub database: String,
    #[prost(string, tag = "2")]
    pub schema: String,
}
