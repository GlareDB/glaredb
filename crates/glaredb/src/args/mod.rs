use anyhow::anyhow;
use anyhow::Result;
use clap::{Parser, ValueEnum};
use std::fmt::Write as _;
use std::path::PathBuf;
use url::Url;
pub mod local;
pub mod server;

pub use {local::*, server::*};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputMode {
    Table,
    Json,
    Ndjson,
    Csv,
}

#[derive(Parser)]
pub struct MetastoreArgs {
    /// TCP address to bind do.
    #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6545"))]
    pub bind: String,

    /// Bucket to use for database catalogs.
    #[clap(short = 'u', long, value_parser)]
    pub bucket: Option<String>,

    /// Path to GCP service account to use when connecting to GCS.
    #[clap(short, long, value_parser)]
    pub service_account_path: Option<String>,

    /// Local file path to store database catalog (for a local persistent
    /// store).
    #[clap(short = 'f', long, value_parser)]
    pub local_file_path: Option<PathBuf>,
}

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

    /// Disable RPC TLS
    ///
    /// (Internal)
    ///
    /// Note: in the future, this will be 'on' by default
    #[clap(long, default_value="true", action = clap::ArgAction::Set, hide = true)]
    pub disable_tls: bool,
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
    #[clap(short = 'o', long, requires = "location", value_parser=parse_key_value_pair)]
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
