use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use protogen::metastore::types::catalog::CatalogState;
use protogen::metastore::types::service::Mutation;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::errors::{CatalogError, Result};

/// Number of outstanding requests per database.

/// Configuration values used when starting up a worker.
#[derive(Debug, Clone, Copy)]
pub struct MetastoreClientConfig {
    /// How often to fetch the latest catalog from Metastore.
    pub fetch_tick_dur: Duration,
    /// Number of ticks with no session references before the worker exits.
    pub max_ticks_before_exit: usize,
}


/// Handle to a metastore client.
#[derive(Debug, Clone)]
pub struct MetastoreClientHandle {
    /// Shared with worker. Used to hint if this client should get the latest
    /// state from the worker.
    ///
    /// Used to prevent unecessary requests and locking.
    pub version_hint: Arc<AtomicU64>,
    pub send: mpsc::Sender<ClientRequest>,
}

impl MetastoreClientHandle {
    /// Get the likely version of the cached catalog state.
    ///
    /// If subsequent calls to this return the same version, it's likely that
    /// the underlying cached state did not change.
    ///
    /// If subsequent calls to this return different versions, it's guaranteed
    /// that the cached state changed.
    pub fn version_hint(&self) -> u64 {
        self.version_hint.load(Ordering::Relaxed)
    }

    /// Ping the worker.
    pub async fn ping(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(ClientRequest::Ping { response: tx }, rx).await
    }

    /// Get the cache the latest state of the catalog.
    pub async fn refresh_cached_state(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(ClientRequest::RefreshCachedState { response: tx }, rx)
            .await
    }

    /// Get the current cached state of the catalog.
    pub async fn get_cached_state(&self) -> Result<Arc<CatalogState>> {
        let (tx, rx) = oneshot::channel();
        self.send(ClientRequest::GetCachedState { response: tx }, rx)
            .await
            .and_then(std::convert::identity) // Flatten
    }

    /// Try to run mutations against the Metastore catalog.
    ///
    /// The version provided should be the version of the catalog state that the
    /// session currently has.
    pub async fn try_mutate(
        &self,
        current_version: u64,
        mutations: Vec<Mutation>,
    ) -> Result<Arc<CatalogState>> {
        let (tx, rx) = oneshot::channel();
        self.send(
            ClientRequest::ExecMutations {
                version: current_version,
                mutations,
                response: tx,
            },
            rx,
        )
        .await
        .and_then(std::convert::identity) // Flatten
    }

    async fn send<R>(&self, req: ClientRequest, rx: oneshot::Receiver<R>) -> Result<R> {
        let tag = req.tag();
        let result = match self.send.try_send(req) {
            Ok(_) => match rx.await {
                Ok(result) => Ok(result),
                Err(_) => Err(CatalogError::new(format!("response channel closed: {tag}"))),
            },
            Err(mpsc::error::TrySendError::Full(_)) => Err(CatalogError::new(format!(
                "response channel overloaded: {tag}"
            ))),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(CatalogError::new(format!("request channel closed: {tag}")))
            }
        };

        // We should be notified in all cases when an error occurs here. These
        // errors indicate our logic for dropping workers is incorrect, or
        // messages are getting stuck somewhere.
        if let Err(e) = &result {
            error!(?e, "failed to make metastore worker request");
        }

        result
    }
}

/// Possible requests to the worker.
pub enum ClientRequest {
    /// Ping the worker, ensuring it's still running.
    Ping { response: oneshot::Sender<()> },

    /// Get the cached catalog state for some database.
    GetCachedState {
        response: oneshot::Sender<Result<Arc<CatalogState>>>,
    },

    /// Execute mutations against a catalog.
    ExecMutations {
        version: u64,
        /// Mutations to send to Metastore.
        mutations: Vec<Mutation>,
        /// Response channel to await for result.
        response: oneshot::Sender<Result<Arc<CatalogState>>>,
    },

    /// Refresh the cached catalog state from persistence for some database
    RefreshCachedState { response: oneshot::Sender<()> },
}

impl ClientRequest {
    fn tag(&self) -> &'static str {
        match self {
            ClientRequest::Ping { .. } => "ping",
            ClientRequest::GetCachedState { .. } => "get_cached_state",
            ClientRequest::ExecMutations { .. } => "exec_mutations",
            ClientRequest::RefreshCachedState { .. } => "refresh_cached_state",
        }
    }
}
