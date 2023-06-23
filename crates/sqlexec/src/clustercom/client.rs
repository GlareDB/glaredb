use crate::clustercom::proto::clustercom::cluster_com_service_client::ClusterComServiceClient;
use crate::clustercom::proto::clustercom::emit_database_event_request::Event;
use crate::clustercom::proto::clustercom::CatalogMutated;
use crate::clustercom::proto::clustercom::EmitDatabaseEventRequest;
use futures::{FutureExt, TryFutureExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ClusterClientError {
    #[error("Cluster error: {0}")]
    Tonic(#[from] tonic::Status),

    #[error("Failed to send on channel")]
    FailedToSend,

    #[error("Failed to receive on channel")]
    FailedToRecv,
}

#[derive(Debug, Clone)]
pub struct ClusterClient {
    _handle: Arc<JoinHandle<()>>,
    send: mpsc::Sender<ClientRequest>,
}

impl ClusterClient {
    pub async fn broadcast_catalog_mutated(&self, db_id: Uuid) -> Result<(), ClusterClientError> {
        let (tx, rx) = oneshot::channel();
        self.send
            .send(ClientRequest::EmitDatabaseMutated {
                db_id,
                response: tx,
            })
            .await
            .map_err(|_| ClusterClientError::FailedToSend)?;

        let _ = rx.await.map_err(|_| ClusterClientError::FailedToRecv)?;

        Ok(())
    }
}

#[derive(Debug)]
enum ClientRequest {
    /// Add or remove members to the cluster.
    MembershipChange {
        to_add: Vec<String>,
        to_remove: Vec<String>,
        response: oneshot::Sender<Result<(), ClusterClientError>>,
    },

    /// Emit a message to all members indicating that a database has been mutated.
    EmitDatabaseMutated {
        db_id: Uuid,
        response: oneshot::Sender<Result<(), ClusterClientError>>,
    },
}

struct ClientWorker {
    /// Bufferred messages to send to other nodes.
    recv: mpsc::Receiver<ClientRequest>,

    /// Map of all clients in the cluster, keyed by address.
    clients: BTreeMap<String, ClusterComServiceClient<Channel>>,
}

impl ClientWorker {
    async fn run(mut self) {
        while let Some(req) = self.recv.recv().await {
            match req {
                ClientRequest::MembershipChange {
                    to_add,
                    to_remove,
                    response,
                } => {
                    unimplemented!()
                }
                ClientRequest::EmitDatabaseMutated { db_id, response } => {
                    let res = self.handle_emit_database_mutate(db_id).await;
                    let _ = response.send(res);
                }
            }
        }
    }

    async fn handle_emit_database_mutate(&mut self, db_id: Uuid) -> Result<(), ClusterClientError> {
        let futs = self.clients.iter_mut().map(|(_, client)| {
            client.emit_database_event(tonic::Request::new(EmitDatabaseEventRequest {
                db_id: db_id.into_bytes().to_vec(),
                event: Some(Event::CatalogMutated(CatalogMutated {})),
            }))
        });

        let _ = futures::future::join_all(futs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
}
