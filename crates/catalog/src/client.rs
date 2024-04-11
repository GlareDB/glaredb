//! Module for facilitating interaction with the Metastore.
//!
//! # Architecture
//!
//! Communication to metastore is facilitated through a supervisor and some
//! background tasks. Each "database" will have its own worker, and each worker
//! will have its own copy of the GRPC client to metastore. Workers also hold a
//! cached copy of the catalog for their respective database.
//!
//! When making a request to metastore, a session will send a request to the
//! worker for the database they belong to, and the worker is responsible for
//! translating and making the actual request to metastore.
//!
//! To illustrate, let's say we have three sessions for the same database:
//!
//! ```text
//! Session 1 -------+
//!                  |
//!                  ▼
//! Session 2 ----> Supervisor/Worker ----> Metastore (HA)
//!                  ▲
//!                  |
//! Session 3 -------+
//! ```
//!
//! Sessions 1, 2, and 3 all have a handle to the supervisor that allows them to
//! submit requests to metastore like getting the updated version of a catalog
//! or mutating a catalog. Each session will initially share a reference to the
//! same cached catalog (Arc).
//!
//! When a session requests a mutation, that request is submitted to the
//! supervisor, then to metastore itself. If metastore successfully mutates the
//! catalog, it will return the updated catalog, and the worker will replace its
//! cached catalog with the updated catalog. The session that made the request
//! will get the updated catalog back from the worker. The other sessions will
//! still have references to the old catalog until they get the updated cached
//! catalog from the worker (which happens during the "prepare" phase of query
//! execution).
//!
//! # Rationale
//!
//! The worker per database model helps us keep the overhead of storing cached
//! catalogs in memory low. By having one worker per database that's responsible
//! for making requests, we're able to store a single copy of the catalog in
//! memory that gets shared across all sessions for a database, cutting down
//! memory usage.
//!
//! Note that executing a single request at a time for a database was the
//! easiest way to accomplish the desired catalog caching behavior, and not due
//! to any limitations in metastore itself.

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

    pub async fn commit_state(
        &self,
        version: u64,
        state: CatalogState,
    ) -> Result<Arc<CatalogState>> {
        let (tx, rx) = oneshot::channel();
        self.send(
            ClientRequest::Commit {
                version,
                state: Arc::new(state),
                response: tx,
            },
            rx,
        )
        .await
        .and_then(std::convert::identity) // Flatten
    }

    /// Try to run mutations against the Metastore catalog and commit them.
    pub async fn try_mutate_and_commit(
        &self,
        current_version: u64,
        mutations: Vec<Mutation>,
    ) -> Result<Arc<CatalogState>> {
        let (tx, rx) = oneshot::channel();
        let state = self
            .send(
                ClientRequest::ExecMutations {
                    version: current_version,
                    mutations,
                    response: tx,
                },
                rx,
            )
            .await??;

        self.commit_state(current_version, state.as_ref().clone())
            .await
    }

    /// Try to run mutations against the Metastore catalog
    /// IMPORTANT: This method does not commit the mutations to the catalog. see `try_mutate_and_commit`
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
    Commit {
        version: u64,
        state: Arc<CatalogState>,
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
            ClientRequest::Commit { .. } => "commit",
            ClientRequest::GetCachedState { .. } => "get_cached_state",
            ClientRequest::ExecMutations { .. } => "exec_mutations",
            ClientRequest::RefreshCachedState { .. } => "refresh_cached_state",
        }
    }
}
