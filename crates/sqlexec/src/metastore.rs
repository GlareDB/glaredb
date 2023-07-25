//! Module for facilitating interaction with the Metastore.
use crate::errors::{ExecError, Result};
use metastore_client::errors::MetastoreClientError;
use metastore_client::proto::service::metastore_service_client::MetastoreServiceClient;
use metastore_client::proto::service::{
    FetchCatalogRequest, InitializeCatalogRequest, MutateRequest,
};
use metastore_client::types::catalog::CatalogState;
use metastore_client::types::service::Mutation;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::{debug_span, error, info, warn, Instrument};
use uuid::Uuid;

/// Number of outstanding requests per database.
const PER_DATABASE_BUFFER: usize = 128;

/// Configuration values used when starting up a worker.
#[derive(Debug, Clone, Copy)]
pub struct WorkerRunConfig {
    /// How often to fetch the latest catalog from Metastore.
    fetch_tick_dur: Duration,
    /// Number of ticks with no session references before the worker exits.
    max_ticks_before_exit: usize,
}

pub const DEFAULT_WORKER_CONFIG: WorkerRunConfig = WorkerRunConfig {
    fetch_tick_dur: Duration::from_secs(60 * 5),
    max_ticks_before_exit: 3,
};

/// Worker client. This client can only make requests for a single database.
#[derive(Debug, Clone)]
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
        self.send(
            WorkerRequest::Ping {
                conn_id: self.conn_id,
                response: tx,
            },
            rx,
        )
        .await
    }

    /// Get the cache the latest state of the catalog.
    pub async fn refresh_cached_state(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(
            WorkerRequest::RefreshCachedState {
                conn_id: self.conn_id,
                response: tx,
            },
            rx,
        )
        .await
    }

    /// Get the current cached state of the catalog.
    pub async fn get_cached_state(&self) -> Result<Arc<CatalogState>> {
        let (tx, rx) = oneshot::channel();
        self.send(
            WorkerRequest::GetCachedState {
                conn_id: self.conn_id,
                response: tx,
            },
            rx,
        )
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
            WorkerRequest::ExecMutations {
                conn_id: self.conn_id,
                version: current_version,
                mutations,
                response: tx,
            },
            rx,
        )
        .await
        .and_then(std::convert::identity) // Flatten
    }

    async fn send<R>(&self, req: WorkerRequest, rx: oneshot::Receiver<R>) -> Result<R> {
        let tag = req.tag();
        let result = match self.send.try_send(req) {
            Ok(_) => match rx.await {
                Ok(result) => Ok(result),
                Err(_) => Err(ExecError::MetastoreResponseChannelClosed {
                    request_type_tag: tag,
                    conn_id: self.conn_id,
                }),
            },
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err(ExecError::MetastoreDatabaseWorkerOverload {
                    request_type_tag: tag,
                    conn_id: self.conn_id,
                })
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(ExecError::MetastoreRequestChannelClosed {
                    request_type_tag: tag,
                    conn_id: self.conn_id,
                })
            }
        };

        // We should be notified in all cases when an error occurs here. These
        // errors indicate our logic for dropping workers is incorrect, or
        // messages are getting stuck somewhere.
        if let Err(e) = &result {
            error!(%e, "failed to make metastore worker request");
        }

        result
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

    /// Refresh the cached catalog state from persistence for some database
    RefreshCachedState {
        conn_id: Uuid,
        response: oneshot::Sender<()>,
    },
}

impl WorkerRequest {
    fn tag(&self) -> &'static str {
        match self {
            WorkerRequest::Ping { .. } => "ping",
            WorkerRequest::GetCachedState { .. } => "get_cached_state",
            WorkerRequest::ExecMutations { .. } => "exec_mutations",
            WorkerRequest::RefreshCachedState { .. } => "refresh_cached_state",
        }
    }

    fn conn_id(&self) -> &Uuid {
        match self {
            WorkerRequest::Ping { conn_id, .. } => conn_id,
            WorkerRequest::GetCachedState { conn_id, .. } => conn_id,
            WorkerRequest::ExecMutations { conn_id, .. } => conn_id,
            WorkerRequest::RefreshCachedState { conn_id, .. } => conn_id,
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
    // TODO: We will want to occasionally clear out dead workers.
    workers: RwLock<HashMap<Uuid, WorkerHandle>>,

    /// Configuration to use for all workers.
    worker_conf: WorkerRunConfig,

    /// GRPC client to metastore. Cloned for each database worker.
    client: MetastoreServiceClient<Channel>,
}

impl Supervisor {
    /// Create a new worker supervisor.
    pub fn new(
        client: MetastoreServiceClient<Channel>,
        worker_conf: WorkerRunConfig,
    ) -> Supervisor {
        Supervisor {
            workers: RwLock::new(HashMap::new()),
            client,
            worker_conf,
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
            handle: tokio::spawn(worker.run(self.worker_conf)),
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

    /// Terminate a worker, waiting until the worker thread finishes.
    ///
    /// Currently only used to test that we can start up a new worker for the
    /// same db.
    #[cfg(test)]
    async fn terminate_worker(&self, db_id: &Uuid) {
        let mut workers = self.workers.write().await;
        if let Some(worker) = workers.remove(db_id) {
            std::mem::drop(workers);
            worker.handle.abort();
            let _ = worker.handle.await;
        }
    }

    /// Check if a worker is dead. Only used in tests.
    #[cfg(test)]
    async fn worker_is_dead(&self, db_id: &Uuid) -> bool {
        let workers = self.workers.read().await;
        match workers.get(db_id) {
            Some(worker) => worker.is_finished(),
            None => true,
        }
    }
}

/// Handle to a database worker.
struct WorkerHandle {
    /// Handle to the database worker.
    handle: JoinHandle<()>,
    /// Version hint shared with clients.
    version_hint: Arc<AtomicU64>,
    /// Sender channel for client requests.
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
    ///
    /// Note that this reference is never changed during the lifetime of the
    /// worker. We can use the strong count to determine the number of active
    /// sessions for this database.
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

    /// Get the number of sessions that are relying on this worker for the
    /// database's cached catalog.
    fn session_count_for_db(&self) -> usize {
        // The worker handle and worker itself have a strong reference to the
        // version. Don't include those when calculating session count.
        //
        // TODO: Figure out why the strong count reference may be less than two.
        // This can happen when running the python tests (so probably related to
        // dropping workers on exit).
        Arc::strong_count(&self.version_hint).saturating_sub(2)
    }

    /// Runs the worker until it exits from inactivity.
    async fn run(mut self, run_conf: WorkerRunConfig) {
        let mut interval = tokio::time::interval(run_conf.fetch_tick_dur);
        let mut num_ticks_no_activity = 0;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let span = debug_span!("fetch_interval", db_id = %self.db_id);
                    self.fetch().instrument(span).await;

                    let sess_count = self.session_count_for_db();
                    info!(%sess_count, db_id = %self.db_id, "worker fetch interval");

                    // If no sessions using the worker, start counting down to
                    // exit. Otherwise reset the counter.
                    if sess_count == 0 {
                        num_ticks_no_activity += 1;
                        if num_ticks_no_activity >= run_conf.max_ticks_before_exit {
                            info!(db_id = %self.db_id, "exiting database worker");
                            return;
                        }
                    } else {
                        num_ticks_no_activity = 0;
                    }
                }

                Some(req) = self.recv.recv() => {
                    let span = debug_span!("handle_request", conn_id = %req.conn_id(), db_id = %self.db_id);
                    self.handle_request(req).instrument(span).await;
                    num_ticks_no_activity = 0;
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
                let result = mutations
                    .into_iter()
                    .map(|m| m.try_into())
                    .collect::<Result<_, _>>();

                let result = match result {
                    Ok(mutations) => {
                        self.client
                            .mutate_catalog(tonic::Request::new(MutateRequest {
                                db_id: self.db_id.into_bytes().to_vec(),
                                catalog_version: version,
                                mutations,
                            }))
                            .await
                    }
                    Err(e) => Err(MetastoreClientError::from(e).into()),
                };

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
            WorkerRequest::RefreshCachedState { response, .. } => {
                self.fetch().await;
                if response.send(()).is_err() {
                    error!("failed to respond to refresh cached catalog state request");
                }
            }
        }
    }

    /// Fetch the latest catalog from Metastore, updating this local catalog cache.
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

#[cfg(test)]
mod tests {
    use super::*;
    use metastore::local::start_inprocess;
    use metastore_client::proto::service::metastore_service_client::MetastoreServiceClient;
    use metastore_client::types::service::{CreateSchema, CreateView, Mutation};
    use object_store::memory::InMemory;
    use tonic::transport::Channel;

    /// Creates a new local Metastore, returning a client connected to that
    /// server.
    ///
    /// The newly created Metastore will have no database data to begin with.
    async fn new_local_metastore() -> MetastoreServiceClient<Channel> {
        start_inprocess(Arc::new(InMemory::new())).await.unwrap()
    }

    #[tokio::test]
    async fn simple_mutate() {
        let client = new_local_metastore().await;

        let supervisor = Supervisor::new(client, DEFAULT_WORKER_CONFIG);

        let conn_id = Uuid::nil();
        let db_id = Uuid::nil();
        let client = supervisor.init_client(conn_id, db_id).await.unwrap();

        let state = client.get_cached_state().await.unwrap();
        let new_state = client
            .try_mutate(
                state.version,
                vec![Mutation::CreateView(CreateView {
                    schema: "public".to_string(),
                    name: "mario".to_string(),
                    sql: "select 1".to_string(),
                    or_replace: false,
                    columns: Vec::new(),
                })],
            )
            .await
            .unwrap();

        assert!(new_state.version > state.version);
    }

    #[tokio::test]
    async fn out_of_date_mutate() {
        let client = new_local_metastore().await;

        let supervisor = Supervisor::new(client, DEFAULT_WORKER_CONFIG);

        let db_id = Uuid::nil();

        let c1 = supervisor.init_client(Uuid::new_v4(), db_id).await.unwrap();
        let c2 = supervisor.init_client(Uuid::new_v4(), db_id).await.unwrap();

        // Both clients have the same state.
        let s1 = c1.get_cached_state().await.unwrap();
        let s2 = c2.get_cached_state().await.unwrap();
        assert_eq!(s1.version, s2.version);

        // Client 1 mutates.
        let _ = c1
            .try_mutate(
                s1.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario".to_string(),
                })],
            )
            .await
            .unwrap();

        // Client 2 fails to mutate because its out of date.
        c2.try_mutate(
            s2.version,
            vec![Mutation::CreateSchema(CreateSchema {
                name: "yoshi".to_string(),
            })],
        )
        .await
        .unwrap_err();

        // Version hint should indicate state was updated due to mutation by
        // client 1.
        assert!(c2.version_hint() > s2.version);

        // Get updated state.
        let s2 = c2.get_cached_state().await.unwrap();

        // Mutation should go through now.
        let _ = c2
            .try_mutate(
                s2.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "yoshi".to_string(),
                })],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn restart_worker() {
        let client = new_local_metastore().await;

        let supervisor = Supervisor::new(client, DEFAULT_WORKER_CONFIG);

        let db_id = Uuid::nil();
        let client = supervisor.init_client(Uuid::new_v4(), db_id).await.unwrap();

        let state = client.get_cached_state().await.unwrap();
        supervisor.terminate_worker(&db_id).await;

        // Worker gone, we should have the only reference.
        assert_eq!(1, Arc::strong_count(&state));

        // But now all requests will error.
        let _ = client.get_cached_state().await.unwrap_err();

        // Initiate a new client, which should spin up a new worker.
        let _ = supervisor.init_client(Uuid::new_v4(), db_id).await.unwrap();
    }

    #[tokio::test]
    async fn worker_exits_on_inactivity() {
        // #984
        logutil::init_test();

        let client = new_local_metastore().await;
        let supervisor = Supervisor::new(
            client,
            WorkerRunConfig {
                fetch_tick_dur: Duration::from_millis(100),
                max_ticks_before_exit: 1,
            },
        );

        let db_id = Uuid::nil();
        let client = supervisor.init_client(Uuid::new_v4(), db_id).await.unwrap();

        client.ping().await.unwrap();
        client.refresh_cached_state().await.unwrap();

        // Worker should still be here after exceeding max ticks.
        tokio::time::sleep(Duration::from_millis(200)).await;
        client.ping().await.unwrap();

        // Drop client, wait for worker to exit.
        std::mem::drop(client);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Worker should no longer be running.
        assert!(supervisor.worker_is_dead(&db_id).await);

        // We should be able to init a new client without issue.
        let client = supervisor.init_client(Uuid::new_v4(), db_id).await.unwrap();
        client.ping().await.unwrap();
    }
}
