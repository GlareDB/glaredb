use crate::errors::{MetastoreError, Result};
use crate::local::{start_inprocess_inmemory, start_inprocess_local};
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
    pub fn new_local(local_path: Option<PathBuf>) -> Self {
        match local_path {
            Some(path) => MetastoreClientMode::LocalDisk { path },
            None => MetastoreClientMode::LocalInMemory,
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
                    fs::create_dir_all(&path).map_err(|e| {
                        MetastoreError::FailedInProcessStartup(format!(
                            "Failed creating directory at path {}: {e}",
                            path.to_string_lossy()
                        ))
                    })?;
                }
                if path.exists() && !path.is_dir() {
                    return Err(MetastoreError::FailedInProcessStartup(format!(
                        "Error creating metastore client, path {} is not a valid directory",
                        path.to_string_lossy()
                    )));
                }
                start_inprocess_local(path).await
            }
            Self::LocalInMemory => start_inprocess_inmemory().await,
        }
    }
}
