//! Module for facilitating interaction with the Metastore.
use crate::errors::{ExecError, Result};
use metastore::proto::service::metastore_service_client::MetastoreServiceClient;
use metastore::proto::service::{FetchCatalogRequest, InitializeCatalogRequest, MutateRequest};
use metastore::types::catalog::CatalogState;
use metastore::types::service::Mutation;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::{debug_span, error, warn, Instrument};
use uuid::Uuid;

/// Number of outstanding requests per database.
const PER_DATABASE_BUFFER: usize = 128;

/// How often to fetch the latest catalog from Metastore.
const FETCH_TICK_DUR: Duration = Duration::from_secs(60 * 5);

/// Worker client. This client can only make requests for a single database.
pub struct SupervisorClient {
    /// Shared with worker. Used to hint if this client should get the latest
    /// state from the worker.
    ///
    /// Used to prevent unecessary requests and locking.
    version_hint: Arc<AtomicU64>,
    conn_id: Uuid,
    send: mpsc::Sender<WorkerRequest>,
}

impl SupervisorClient {
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
        if self
            .send
            .send(WorkerRequest::Ping {
                conn_id: self.conn_id,
                response: tx,
            })
            .await
            .is_err()
        {
            return Err(ExecError::MetastoreDatabaseWorkerOverload);
        }
        match rx.await {
            Ok(_) => Ok(()),
            Err(_) => Err(ExecError::MetastoreDatabaseWorkerOverload),
        }
    }

    /// Get the current cached state of the catalog.
    pub async fn get_cached_state(&self) -> Result<Arc<CatalogState>> {
        let (tx, rx) = oneshot::channel();
        if self
            .send
            .send(WorkerRequest::GetCachedState {
                conn_id: self.conn_id,
                response: tx,
            })
            .await
            .is_err()
        {
            return Err(ExecError::MetastoreDatabaseWorkerOverload);
        }
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(ExecError::MetastoreDatabaseWorkerOverload),
        }
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
        if self
            .send
            .send(WorkerRequest::ExecMutations {
                conn_id: self.conn_id,
                version: current_version,
                mutations,
                response: tx,
            })
            .await
            .is_err()
        {
            return Err(ExecError::MetastoreDatabaseWorkerOverload);
        }
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(ExecError::MetastoreDatabaseWorkerOverload),
        }
    }
}

/// Possible requests to the worker.
enum WorkerRequest {
    /// Ping the worker, ensuring it's still running.
    Ping {
        conn_id: Uuid,
        response: oneshot::Sender<()>,
    },

    /// Get the cached catalog state for some database.
    GetCachedState {
        conn_id: Uuid,
        response: oneshot::Sender<Result<Arc<CatalogState>>>,
    },

    /// Execute mutations against a catalog.
    ExecMutations {
        conn_id: Uuid,
        version: u64,
        /// Mutations to send to Metastore.
        mutations: Vec<Mutation>,
        /// Response channel to await for result.
        response: oneshot::Sender<Result<Arc<CatalogState>>>,
    },
}

impl WorkerRequest {
    fn conn_id(&self) -> &Uuid {
        match self {
            WorkerRequest::Ping { conn_id, .. } => conn_id,
            WorkerRequest::GetCachedState { conn_id, .. } => conn_id,
            WorkerRequest::ExecMutations { conn_id, .. } => conn_id,
        }
    }
}

/// Supervisor of all database workers.
pub struct Supervisor {
    /// All database workers.
    ///
    /// Note that our current architecture has a one-to-one mapping with
    /// database to node, but this would make it very easy to support
    /// multi-tenant.
    workers: RwLock<HashMap<Uuid, WorkerHandle>>,

    /// GRPC client to metastore. Cloned for each database worker.
    client: MetastoreServiceClient<Channel>,
}

impl Supervisor {
    /// Create a new worker supervisor.
    pub fn new(client: MetastoreServiceClient<Channel>) -> Supervisor {
        Supervisor {
            workers: RwLock::new(HashMap::new()),
            client,
        }
    }

    /// Initialize a client for a single database.
    ///
    /// This will initialize a database worker as appropriate.
    pub async fn init_client(&self, conn_id: Uuid, db_id: Uuid) -> Result<SupervisorClient> {
        let client = self.init_client_inner(conn_id, db_id).await?;
        match client.ping().await {
            Ok(_) => Ok(client),
            Err(_) => {
                // This may happen if tokio is delayed in marking a handle as
                // finished, and we try to send on an mpsc channel that's been
                // closed. This should be rare.
                warn!("client failed ping, recreating client");
                let client = self.init_client_inner(conn_id, db_id).await?;
                client.ping().await?;
                Ok(client)
            }
        }
    }

    async fn init_client_inner(&self, conn_id: Uuid, db_id: Uuid) -> Result<SupervisorClient> {
        // Fast path, already have a worker.
        {
            let workers = self.workers.read().await;
            match workers.get(&db_id) {
                Some(worker) if !worker.is_finished() => {
                    return Ok(SupervisorClient {
                        version_hint: worker.version_hint.clone(),
                        conn_id,
                        send: worker.send.clone(),
                    });
                }
                _ => (), // Continue on.
            }
        }

        // Slow path, need to initialize a worker.
        let (worker, send) = DatabaseWorker::init(db_id, self.client.clone()).await?;

        let mut workers = self.workers.write().await;
        // Raced or the worker is finished.
        match workers.get(&db_id) {
            Some(worker) if !worker.is_finished() => {
                return Ok(SupervisorClient {
                    version_hint: worker.version_hint.clone(),
                    conn_id,
                    send: worker.send.clone(),
                });
            }
            _ => (), // Continue on.
        }

        // Insert handle.
        let version_hint = worker.version_hint.clone();
        let handle = WorkerHandle {
            handle: tokio::spawn(worker.run()),
            version_hint: version_hint.clone(),
            send: send.clone(),
        };
        workers.insert(db_id, handle);

        Ok(SupervisorClient {
            version_hint,
            conn_id,
            send,
        })
    }
}

/// Handle to a database worker.
struct WorkerHandle {
    handle: JoinHandle<()>,
    version_hint: Arc<AtomicU64>,
    send: mpsc::Sender<WorkerRequest>,
}

impl WorkerHandle {
    /// Check if a handle to a worker is done.
    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

/// Worker for a single database.
struct DatabaseWorker {
    /// ID of the database we're working with.
    db_id: Uuid,

    /// Version of the latest catalog. This provides a hint to clients if they
    /// should update their catalog state.
    ///
    /// This should be updated after setting the cached state.
    version_hint: Arc<AtomicU64>,

    /// Cached catalog state for the database.
    cached_state: Arc<CatalogState>,

    /// GRPC client to metastore.
    client: MetastoreServiceClient<Channel>,

    /// Receive requests from sessions.
    recv: mpsc::Receiver<WorkerRequest>,
}

impl DatabaseWorker {
    /// Intialize a worker for a database.
    async fn init(
        db_id: Uuid,
        mut client: MetastoreServiceClient<Channel>,
    ) -> Result<(DatabaseWorker, mpsc::Sender<WorkerRequest>)> {
        let _ = client
            .initialize_catalog(tonic::Request::new(InitializeCatalogRequest {
                db_id: db_id.into_bytes().to_vec(),
            }))
            .await?;

        let resp = client
            .fetch_catalog(tonic::Request::new(FetchCatalogRequest {
                db_id: db_id.into_bytes().to_vec(),
            }))
            .await?;
        let resp = resp.into_inner();

        let catalog: CatalogState = match resp.catalog {
            Some(c) => c.try_into()?,
            None => return Err(ExecError::Internal("missing field: 'catalog'".to_string())),
        };

        let (send, recv) = mpsc::channel(PER_DATABASE_BUFFER);

        Ok((
            DatabaseWorker {
                db_id,
                version_hint: Arc::new(AtomicU64::new(catalog.version)),
                cached_state: Arc::new(catalog),
                client,
                recv,
            },
            send,
        ))
    }

    async fn run(mut self) {
        let mut interval = tokio::time::interval(FETCH_TICK_DUR);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // TODO: After some number of ticks without client
                    // interaction, we should exit so as to release cached
                    // states not in use.
                    let span = debug_span!("fetch_interval", db_id = %self.db_id);
                    self.fetch().instrument(span).await;
                }

                Some(req) = self.recv.recv() => {
                    let span = debug_span!("handle_request", conn_id = %req.conn_id(), db_id = %self.db_id);
                    self.handle_request(req).instrument(span).await;
                }
            }
        }
    }

    /// Handle a client request.
    ///
    /// If a mutate results in a newer catalog being returned from Metastore,
    /// the local cache will be updated with that new catalog.
    async fn handle_request(&mut self, req: WorkerRequest) {
        match req {
            WorkerRequest::Ping { response, .. } => {
                if response.send(()).is_err() {
                    error!("failed to respond to ping");
                }
            }
            WorkerRequest::GetCachedState { response, .. } => {
                if response.send(Ok(self.cached_state.clone())).is_err() {
                    error!("failed to send cached state");
                }
            }
            WorkerRequest::ExecMutations {
                version,
                mutations,
                response,
                ..
            } => {
                let result = self
                    .client
                    .mutate_catalog(tonic::Request::new(MutateRequest {
                        db_id: self.db_id.into_bytes().to_vec(),
                        catalog_version: version,
                        mutations: mutations.into_iter().map(|m| m.into()).collect(),
                    }))
                    .await;

                let result = match result {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        // TODO: Properly check if we updated.
                        match resp.catalog {
                            Some(catalog) => {
                                // Update this worker's cache.
                                let state: CatalogState = catalog.try_into().unwrap(); // TODO
                                self.set_cached_state(state);
                            }
                            _ => error!("missing catalog state"),
                        }
                        Ok(self.cached_state.clone())
                    }
                    Err(e) => Err(e.into()),
                };

                if response.send(result).is_err() {
                    error!("failed to send result of mutate");
                }
            }
        }
    }

    /// Fetch the latest catalog from Metastore, updating this local catalog
    /// cache.
    async fn fetch(&mut self) {
        match self
            .client
            .fetch_catalog(tonic::Request::new(FetchCatalogRequest {
                db_id: self.db_id.into_bytes().to_vec(),
            }))
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let catalog: CatalogState = match resp.catalog {
                    Some(c) => match c.try_into() {
                        Ok(c) => c,
                        Err(e) => {
                            error!(%e, "failed to convert catalog state");
                            return;
                        }
                    },
                    None => {
                        error!("missing catalog on response");
                        return;
                    }
                };
                self.set_cached_state(catalog);
            }
            Err(e) => error!(%e, "failed to fetch catalog"),
        }
    }

    /// Set the cached catalog state for this database.
    fn set_cached_state(&mut self, state: CatalogState) {
        self.cached_state = Arc::new(state);
        self.version_hint
            .store(self.cached_state.version, Ordering::Relaxed);
    }
}
