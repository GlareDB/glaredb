use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub rpc_tls: Option<TlsConfig>,
    pub rpc_client_tls: Option<TlsClientConfig>,
}

#[derive(Deserialize)]
pub struct TlsConfig {
    pub server_cert_path: String,
    pub server_key_path: String,
}

#[derive(Deserialize)]
pub struct TlsClientConfig {
    pub ca_cert_path: String,
    pub domain: String,
}
