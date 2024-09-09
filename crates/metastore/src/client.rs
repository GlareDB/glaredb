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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use catalog::client::{ClientRequest, MetastoreClientConfig, MetastoreClientHandle};
use catalog::errors::CatalogError;
use protogen::gen::metastore::service::metastore_service_client::MetastoreServiceClient;
use protogen::gen::metastore::service::{CommitRequest, FetchCatalogRequest, MutateRequest};
use protogen::metastore::types::catalog::CatalogState;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::{debug, debug_span, error, warn, Instrument};
use uuid::Uuid;

use crate::errors::Result;

/// Number of outstanding requests per database.
const PER_DATABASE_BUFFER: usize = 128;

pub const DEFAULT_METASTORE_CLIENT_CONFIG: MetastoreClientConfig = MetastoreClientConfig {
    fetch_tick_dur: Duration::from_secs(60 * 5),
    max_ticks_before_exit: 3,
};

/// Handle to a metastore client.

/// Supervisor of all database workers.
pub struct MetastoreClientSupervisor {
    /// All database workers keyed by database ids.
    // TODO: We will want to occasionally clear out dead workers.
    workers: RwLock<HashMap<Uuid, StatefulWorkerHandle>>,

    /// Configuration to use for all workers.
    worker_conf: MetastoreClientConfig,

    /// GRPC client to metastore. Cloned for each database worker.
    client: MetastoreServiceClient<Channel>,
}

impl MetastoreClientSupervisor {
    /// Create a new worker supervisor.
    pub fn new(
        client: MetastoreServiceClient<Channel>,
        worker_conf: catalog::client::MetastoreClientConfig,
    ) -> MetastoreClientSupervisor {
        MetastoreClientSupervisor {
            workers: RwLock::new(HashMap::new()),
            client,
            worker_conf,
        }
    }

    /// Initialize a client for a single database.
    ///
    /// This will initialize a database worker as appropriate.
    pub async fn init_client(&self, db_id: Uuid) -> Result<MetastoreClientHandle> {
        let client = self.init_client_inner(db_id).await?;
        match client.ping().await {
            Ok(_) => Ok(client),
            Err(_) => {
                // This may happen if tokio is delayed in marking a handle as
                // finished, and we try to send on an mpsc channel that's been
                // closed. This should be rare.
                warn!("client failed ping, recreating client");
                let client = self.init_client_inner(db_id).await?;
                client.ping().await.unwrap();
                Ok(client)
            }
        }
    }

    async fn init_client_inner(&self, db_id: Uuid) -> Result<MetastoreClientHandle> {
        // Fast path, already have a worker.
        {
            let workers = self.workers.read().await;
            match workers.get(&db_id) {
                Some(worker) if !worker.is_finished() => {
                    return Ok(MetastoreClientHandle {
                        version_hint: worker.version_hint.clone(),
                        send: worker.send.clone(),
                    });
                }

                _ => (), // Continue on.
            }
        }

        // Slow path, need to initialize a worker.
        let (worker, send) = StatefulWorker::init(db_id, self.client.clone()).await?;

        let mut workers = self.workers.write().await;
        // Raced or the worker is finished.
        match workers.get(&db_id) {
            Some(worker) if !worker.is_finished() => {
                return Ok(MetastoreClientHandle {
                    version_hint: worker.version_hint.clone(),
                    send: worker.send.clone(),
                });
            }
            _ => (), // Continue on.
        }

        // Insert handle.
        let version_hint = worker.version_hint.clone();
        let handle = StatefulWorkerHandle {
            handle: tokio::spawn(worker.run(self.worker_conf)),
            version_hint: version_hint.clone(),
            send: send.clone(),
        };
        workers.insert(db_id, handle);

        Ok(MetastoreClientHandle { version_hint, send })
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
struct StatefulWorkerHandle {
    /// Handle to the database worker.
    handle: JoinHandle<()>,
    /// Version hint shared with clients.
    version_hint: Arc<AtomicU64>,
    /// Sender channel for client requests.
    send: mpsc::Sender<ClientRequest>,
}

impl StatefulWorkerHandle {
    /// Check if a handle to a worker is done.
    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

/// Stateful client for a single database.
struct StatefulWorker {
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
    recv: mpsc::Receiver<ClientRequest>,
}

impl StatefulWorker {
    /// Intialize a worker for a database.
    async fn init(
        db_id: Uuid,
        mut client: MetastoreServiceClient<Channel>,
    ) -> Result<(StatefulWorker, mpsc::Sender<ClientRequest>)> {
        let resp = client
            .fetch_catalog(tonic::Request::new(FetchCatalogRequest {
                db_id: db_id.into_bytes().to_vec(),
            }))
            .await
            .unwrap();
        let resp = resp.into_inner();

        let catalog: CatalogState = match resp.catalog {
            Some(c) => c.try_into()?,
            None => return Err(CatalogError::new("missing field: 'catalog'").into()),
        };

        let (send, recv) = mpsc::channel(PER_DATABASE_BUFFER);

        Ok((
            StatefulWorker {
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
    async fn run(mut self, run_conf: MetastoreClientConfig) {
        let mut interval = tokio::time::interval(run_conf.fetch_tick_dur);
        let mut num_ticks_no_activity = 0;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let span = debug_span!("fetch_interval", db_id = %self.db_id);
                    self.fetch().instrument(span).await;

                    let sess_count = self.session_count_for_db();
                    debug!(%sess_count, db_id = %self.db_id, "worker fetch interval");

                    // If no sessions using the worker, start counting down to
                    // exit. Otherwise reset the counter.
                    if sess_count == 0 {
                        num_ticks_no_activity += 1;
                        if num_ticks_no_activity >= run_conf.max_ticks_before_exit {
                            debug!(db_id = %self.db_id, "exiting database worker");
                            return;
                        }
                    } else {
                        num_ticks_no_activity = 0;
                    }
                }

                Some(req) = self.recv.recv() => {
                    let span = debug_span!("handle_request", db_id = %self.db_id);
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
    async fn handle_request(&mut self, req: ClientRequest) {
        match req {
            ClientRequest::Ping { response, .. } => {
                if response.send(()).is_err() {
                    error!("failed to respond to ping");
                }
            }
            ClientRequest::Commit {
                version: current_catalog_version,
                state,
                response,
            } => {
                let state: CatalogState = state.as_ref().clone();
                if state.version <= current_catalog_version {
                    let e =  Err(CatalogError::new(format!(
                        "cannot commit outdated state: state version: {}, current catalog version: {}",
                        state.version, current_catalog_version
                    )));
                    return response.send(e).unwrap();
                }

                let commit_request = tonic::Request::new(CommitRequest {
                    db_id: self.db_id.into_bytes().to_vec(),
                    catalog_version: current_catalog_version,
                    catalog: Some(state.try_into().unwrap()),
                });

                let result = self.client.commit_catalog(commit_request).await.unwrap();
                let result = result.into_inner();
                let state: CatalogState = result.catalog.unwrap().try_into().unwrap();
                self.set_cached_state(state);
                let res = Ok(self.cached_state.clone());

                if response.send(res).is_err() {
                    error!("failed to respond to commit");
                }
            }
            ClientRequest::GetCachedState { response, .. } => {
                if response.send(Ok(self.cached_state.clone())).is_err() {
                    error!("failed to send cached state");
                }
            }
            ClientRequest::ExecMutations {
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
                    Ok(mutations) => self
                        .client
                        .mutate_catalog(tonic::Request::new(MutateRequest {
                            db_id: self.db_id.into_bytes().to_vec(),
                            catalog_version: version,
                            mutations,
                        }))
                        .await
                        .map_err(CatalogError::from),
                    Err(e) => Err(CatalogError::new(e.to_string())),
                };
                let result = match result {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        // TODO: Properly check if we updated.
                        match resp.catalog {
                            Some(catalog) => {
                                let state: CatalogState = catalog.try_into().unwrap(); // TODO
                                Ok(Arc::new(state))
                            }
                            _ => {
                                panic!("missing catalog state")
                            }
                        }
                    }
                    Err(e) => Err(e),
                };

                if let Err(result) = response.send(result.map_err(Into::into)) {
                    match result {
                        Ok(_) => error!("failed to send ok result of mutate"),
                        Err(e) => error!(%e, "failed to send error result of mutate"),
                    }
                }
            }
            ClientRequest::RefreshCachedState { response, .. } => {
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
            Err(e) => error!(?e, "failed to fetch catalog"),
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
    use object_store::memory::InMemory;
    use protogen::gen::metastore::service::metastore_service_client::MetastoreServiceClient;
    use protogen::metastore::types::service::{CreateSchema, CreateView, Mutation};
    use tonic::transport::Channel;

    use super::*;
    use crate::local::start_inprocess;

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

        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);

        let db_id = Uuid::nil();
        let client = supervisor.init_client(db_id).await.unwrap();

        let state = client.get_cached_state().await.unwrap();
        let new_state = client
            .try_mutate_and_commit(
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

        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);
        let db_id = Uuid::nil();

        let c1 = supervisor.init_client(db_id).await.unwrap();
        let c2 = supervisor.init_client(db_id).await.unwrap();

        // Both clients have the same state.
        let s1 = c1.get_cached_state().await.unwrap();
        let s2 = c2.get_cached_state().await.unwrap();
        assert_eq!(s1.version, s2.version);

        // Client 1 mutates.
        let _ = c1
            .try_mutate_and_commit(
                s1.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        // Client 2 fails to mutate because its out of date.
        c2.try_mutate_and_commit(
            s2.version,
            vec![Mutation::CreateSchema(CreateSchema {
                name: "yoshi".to_string(),
                if_not_exists: false,
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
            .try_mutate_and_commit(
                s2.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "yoshi".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn restart_worker() {
        let client = new_local_metastore().await;

        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);

        let db_id = Uuid::nil();
        let client = supervisor.init_client(db_id).await.unwrap();

        let state = client.get_cached_state().await.unwrap();
        supervisor.terminate_worker(&db_id).await;

        // Worker gone, we should have the only reference.
        assert_eq!(1, Arc::strong_count(&state));

        // But now all requests will error.
        let _ = client.get_cached_state().await.unwrap_err();

        // Initiate a new client, which should spin up a new worker.
        let _ = supervisor.init_client(db_id).await.unwrap();
    }

    #[tokio::test]
    async fn worker_exits_on_inactivity() {
        // #984
        logutil::init_test();

        let client = new_local_metastore().await;
        let supervisor = MetastoreClientSupervisor::new(
            client,
            MetastoreClientConfig {
                fetch_tick_dur: Duration::from_millis(100),
                max_ticks_before_exit: 1,
            },
        );

        let db_id = Uuid::nil();
        let client = supervisor.init_client(db_id).await.unwrap();

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
        let client = supervisor.init_client(db_id).await.unwrap();
        client.ping().await.unwrap();
    }
    #[tokio::test]
    async fn mutate_no_commit() {
        // Test to ensure uncommitted states aren't accessible.

        let client = new_local_metastore().await;
        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);

        let db_id = Uuid::nil();
        let c1 = supervisor.init_client(db_id).await.unwrap();
        let c2 = supervisor.init_client(db_id).await.unwrap();

        // Both clients have the same state.
        let s1 = c1.get_cached_state().await.unwrap();
        let s2 = c2.get_cached_state().await.unwrap();
        assert_eq!(s1.version, s2.version);

        // Client 1 mutates, no commit.
        let _ = c1
            .try_mutate(
                s1.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        // Client 2 should still get latest committed version.
        c2.refresh_cached_state().await.unwrap();
        let refreshed2 = c2.get_cached_state().await.unwrap();
        assert_eq!(s2.version, refreshed2.version);

        // Client 1 should also get the latest committed version, not the
        // uncommitted version.
        c1.refresh_cached_state().await.unwrap();
        let refreshed1 = c1.get_cached_state().await.unwrap();
        assert_eq!(s1.version, refreshed1.version);
    }

    #[tokio::test]
    async fn double_mutate() {
        // Test to ensure a client can mutate twice without having the
        // intermediate mutate affect anything.
        //
        // This represents the "rollback" case for when a query fails and
        // doesn't commit.

        let client = new_local_metastore().await;
        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);

        let db_id = Uuid::nil();
        let c = supervisor.init_client(db_id).await.unwrap();
        let state = c.get_cached_state().await.unwrap();

        // First mutation. This one doesn't get committed.
        let _ = c
            .try_mutate(
                state.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        // Second mutation, this one gets committed.
        let new_state = c
            .try_mutate(
                state.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "mario".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        let _ = c
            .commit_state(state.version, new_state.as_ref().clone())
            .await
            .unwrap();

        // Client should now have the new committed state.
        c.refresh_cached_state().await.unwrap();
        let refreshed = c.get_cached_state().await.unwrap();
        assert_eq!(new_state.version, refreshed.version);

        // New state version should not be the same as the original version.
        assert_ne!(state.version, refreshed.version);
    }

    #[tokio::test]
    async fn commit_arbitrary_state_version() {
        // Test trying to commit to an arbitrary version.

        let client = new_local_metastore().await;
        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);

        let db_id = Uuid::nil();
        let c = supervisor.init_client(db_id).await.unwrap();
        let state = c.get_cached_state().await.unwrap();

        // Mutate...
        let new_state = c
            .try_mutate(
                state.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        // Then commit with arbitrary version.
        let _ = c
            .commit_state(9999, new_state.as_ref().clone())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn concurrent_commits() {
        // Test to ensure two clients committing at the same errors as
        // appropriate.

        let client = new_local_metastore().await;
        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);

        let db_id = Uuid::nil();
        let c1 = supervisor.init_client(db_id).await.unwrap();
        let c2 = supervisor.init_client(db_id).await.unwrap();

        // Both clients have the same state.
        let s1 = c1.get_cached_state().await.unwrap();
        let s2 = c2.get_cached_state().await.unwrap();
        assert_eq!(s1.version, s2.version);

        // Client 1 mutation...
        let new_state1 = c1
            .try_mutate(
                s1.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario1".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        // Client 2 mutation...
        let new_state2 = c2
            .try_mutate(
                s2.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario2".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        // Client 1 commit should succeed.
        let _ = c1
            .commit_state(s1.version, new_state1.as_ref().clone())
            .await
            .unwrap();

        // Client 2 commit should error.
        let _ = c2
            .commit_state(s2.version, new_state2.as_ref().clone())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn error_on_commit_to_committed_catalog() {
        // Test to ensure we error when trying to commit an already committed
        // catalog.
        //
        // Once committed, a catalog version shouldn't be committed to again.
        // Other wise we could end up in a scenario where two nodes commit the
        // same version, with one overwriting the other's version.

        let client = new_local_metastore().await;
        let supervisor = MetastoreClientSupervisor::new(client, DEFAULT_METASTORE_CLIENT_CONFIG);

        let db_id = Uuid::nil();
        let c = supervisor.init_client(db_id).await.unwrap();
        let state = c.get_cached_state().await.unwrap();

        // Mutate...
        let new_state = c
            .try_mutate(
                state.version,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "wario1".to_string(),
                    if_not_exists: false,
                })],
            )
            .await
            .unwrap();

        // Commit as normal.
        let _ = c
            .commit_state(state.version, new_state.as_ref().clone())
            .await
            .unwrap();

        // Try to commit again.
        c.refresh_cached_state().await.unwrap();
        let refreshed = c.get_cached_state().await.unwrap();
        let _ = c
            .commit_state(refreshed.version, new_state.as_ref().clone())
            .await
            .unwrap_err();
    }
}
