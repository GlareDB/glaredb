use prost::Message;

use crate::gen::metastore::options::TunnelOptions;

#[derive(Clone, PartialEq, Message)]
pub struct PostgresAccess {
    #[prost(string, tag = "1")]
    pub conn_str: String,
    #[prost(message, optional, tag = "2")]
    pub tunnel: Option<TunnelOptions>,
}
