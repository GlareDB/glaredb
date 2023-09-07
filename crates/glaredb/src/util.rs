use anyhow::{anyhow, Result};
use metastore::local::{start_inprocess_inmemory, start_inprocess_local};
use protogen::gen::metastore::service::metastore_service_client::MetastoreServiceClient;
use std::path::PathBuf;
use std::{fs, time::Duration};
use tonic::transport::{Channel, Endpoint};
use tracing::info;

/// Determine how to connect to metastore.
#[derive(Debug)]
pub enum MetastoreClientMode {
    /// Connect to a remote metastore.
    Remote { addr: String },
    /// Start an in process metastore backed by files at some path.
    LocalDisk { path: PathBuf },
    /// Start an in process metastore that persists nothing.
    LocalInMemory,
}

impl MetastoreClientMode {
    pub fn new_local(local_path: Option<PathBuf>) -> Result<Self> {
        match local_path {
            Some(path) => Ok(MetastoreClientMode::LocalDisk { path }),
            None => Ok(MetastoreClientMode::LocalInMemory),
        }
    }
    pub fn new_from_options(addr: Option<String>, local_path: Option<PathBuf>) -> Result<Self> {
        match (addr, &local_path) {
            (Some(_), Some(_)) => Err(anyhow!(
                "Only one of metastore address or metastore path may be provided."
            )),
            (Some(addr), None) => Ok(MetastoreClientMode::Remote { addr }),
            _ => Self::new_local(local_path),
        }
    }

    /// Create a new metastore client.
    pub async fn into_client(self) -> Result<MetastoreServiceClient<Channel>> {
        match self {
            MetastoreClientMode::Remote { addr } => {
                info!(%addr, "connecting to remote metastore");
                let channel = Endpoint::new(addr)?
                    .tcp_keepalive(Some(Duration::from_secs(600)))
                    .tcp_nodelay(true)
                    .keep_alive_while_idle(true)
                    .connect()
                    .await?;
                Ok(MetastoreServiceClient::new(channel))
            }
            Self::LocalDisk { path } => {
                if !path.exists() {
                    fs::create_dir_all(&path)?;
                }
                if path.exists() && !path.is_dir() {
                    return Err(anyhow!("Path is not a valid directory"));
                }
                Ok(start_inprocess_local(path).await?)
            }
            Self::LocalInMemory => Ok(start_inprocess_inmemory().await?),
        }
    }
}
