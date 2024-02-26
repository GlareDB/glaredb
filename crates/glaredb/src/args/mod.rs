use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};

use crate::proxy::TLSMode;

pub mod local;
pub mod server;
pub mod slt;
pub use local::*;
pub use server::*;
pub use slt::*;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputMode {
    Table,
    Json,
    Ndjson,
    Csv,
}

#[derive(Parser)]
pub struct MetastoreArgs {}

#[derive(Parser)]
pub struct RpcProxyArgs {
    /// TCP address to bind to.
    #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6444"))]
    pub bind: String,

    /// Address of the GlareDB cloud server.
    #[clap(long)]
    pub cloud_api_addr: String,

    /// Authorization code for communicating with Cloud.
    #[clap(long)]
    pub cloud_auth_code: String,

    /// Mode for TLS Validation (required, optional, disabled)
    ///
    /// (Internal)
    #[clap(long, value_enum, hide = true)]
    pub tls_mode: TLSMode,

    /// Path to TLS server cert to use.
    #[clap(long, default_value = "/etc/certs/tls.crt")]
    pub tls_cert_path: String,

    /// Path to TLS server key to use.
    #[clap(long, default_value = "/etc/certs/tls.key")]
    pub tls_key_path: String,
}

#[derive(Parser)]
pub struct PgProxyArgs {
    /// TCP address to bind to.
    #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6544"))]
    pub bind: String,

    /// Path to SSL server cert to use.
    #[clap(long)]
    pub ssl_server_cert: Option<String>,

    /// Path to SSL server key to use.
    #[clap(long)]
    pub ssl_server_key: Option<String>,

    /// Address of the GlareDB cloud server.
    #[clap(long)]
    pub cloud_api_addr: String,

    /// Authorization code for communicating with Cloud.
    #[clap(long)]
    pub cloud_auth_code: String,
}

#[derive(Debug, Clone, Parser)]
pub struct StorageConfigArgs {
    /// URL of the object store in which to keep the data in.
    #[clap(short, long)]
    pub location: Option<String>,

    /// Storage options for building the object store.
    #[clap(short = 'o', long = "option", requires = "location", value_parser=parse_key_value_pair)]
    pub storage_options: Vec<(String, String)>,
}

fn parse_key_value_pair(key_value_pair: &str) -> Result<(String, String)> {
    key_value_pair
        .split_once('=')
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .ok_or(anyhow!(
            "Expected key-value pair delimited by an equals sign, got '{key_value_pair}'"
        ))
}
