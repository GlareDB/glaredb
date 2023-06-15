use anyhow::{anyhow, Result};
use metastore::local::{start_inprocess_inmemory, start_inprocess_local};
use metastoreproto::proto::service::metastore_service_client::MetastoreServiceClient;
use std::fs;
use std::path::{Path, PathBuf};
use tonic::transport::Channel;
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
    pub fn new_from_options(addr: Option<String>, local_path: Option<PathBuf>) -> Result<Self> {
        match (addr, local_path) {
            (Some(_), Some(_)) => Err(anyhow!(
                "Only one of metastore address or metastore path may be provided."
            )),
            (Some(addr), None) => Ok(MetastoreClientMode::Remote { addr }),
            (_, Some(path)) => Ok(MetastoreClientMode::LocalDisk { path }),
            (_, _) => Ok(MetastoreClientMode::LocalInMemory),
        }
    }

    /// Create a new metastore client.
    pub async fn into_client(self) -> Result<MetastoreServiceClient<Channel>> {
        match self {
            MetastoreClientMode::Remote { addr } => {
                info!(%addr, "connecting to remote metastore");
                Ok(MetastoreServiceClient::connect(addr).await?)
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

/// Ensure that the spill path exists and that it's writable if provided.
pub fn ensure_spill_path<P: AsRef<Path>>(path: Option<P>) -> Result<()> {
    if let Some(p) = path {
        let path = p.as_ref();
        info!(?path, "checking spill path");

        fs::create_dir_all(path)?;

        let file = path.join("glaredb_startup_spill_check");
        fs::write(&file, vec![0, 1, 2, 3])?;
        fs::remove_file(&file)?;
    }
    Ok(())
}
