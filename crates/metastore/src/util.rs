use std::path::PathBuf;

use ioutil::ensure_dir;
use protogen::gen::metastore::service::metastore_service_client::MetastoreServiceClient;
use tonic::transport::Channel;

use crate::errors::Result;
use crate::local::{start_inprocess_inmemory, start_inprocess_local};

/// Determine how to connect to metastore.
#[derive(Debug)]
pub enum MetastoreClientMode {
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
            // MetastoreClientMode::Remote { addr } => {
            //     info!(%addr, "connecting to remote metastore");
            //     let channel = Endpoint::new(addr)?
            //         .tcp_keepalive(Some(Duration::from_secs(600)))
            //         .tcp_nodelay(true)
            //         .keep_alive_while_idle(true)
            //         .connect()
            //         .await?;
            //     Ok(MetastoreServiceClient::new(channel))
            // }
            Self::LocalDisk { path } => {
                ensure_dir(&path)?;
                start_inprocess_local(path).await
            }
            Self::LocalInMemory => start_inprocess_inmemory().await,
        }
    }
}
