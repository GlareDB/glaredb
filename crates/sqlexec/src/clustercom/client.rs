use crate::clustercom::proto::clustercom::cluster_com_service_client::ClusterComServiceClient;
use crate::clustercom::proto::clustercom::emit_database_event_request::Event;
use crate::clustercom::proto::clustercom::CatalogMutated;
use crate::clustercom::proto::clustercom::EmitDatabaseEventRequest;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ClusterClientError {
    #[error("Cluster error: {0}")]
    Tonic(#[from] tonic::Status),

    #[error("Failed to send on channel")]
    FailedToSend,

    #[error("Failed to receive on channel")]
    FailedToRecv,

    #[error(transparent)]
    TonicTransport(#[from] tonic::transport::Error),
}

/// Stores clients to other members in the cluster.
///
/// By default, there will be no clients until `membership_change` is called.
#[derive(Debug, Clone, Default)]
pub struct ClusterClient {
    /// Map of all clients in the cluster, keyed by address.
    clients: Arc<RwLock<BTreeMap<String, ClusterComServiceClient<Channel>>>>,
}

impl ClusterClient {
    /// Broadcast that catalog has been mutated to all nodes.
    pub async fn broadcast_catalog_mutated(&self, db_id: Uuid) -> Result<(), ClusterClientError> {
        let mut clients: Vec<_> = {
            let clients = self.clients.read();
            clients.values().cloned().collect()
        };

        let futs = clients.iter_mut().map(|client| {
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

    /// Change the members that this client knows abouts.
    ///
    /// On new members, a client is will be opened. Members that are removed
    /// will be dropped.
    pub async fn membership_change(
        &self,
        to_add: Vec<String>,
        to_remove: Vec<String>,
    ) -> Result<(), ClusterClientError> {
        let to_add: Vec<_> = {
            let clients = self.clients.read();
            to_add
                .into_iter()
                .filter(|addr| !clients.contains_key(addr))
                .collect()
        };

        let mut to_add_clients = Vec::with_capacity(to_add.len());
        for addr in to_add {
            // TODO: Probably do these in parallel.
            // TODO: Also don't bail after the first error.
            let client = ClusterComServiceClient::connect(addr.clone()).await?;
            to_add_clients.push((addr, client));
        }

        let mut clients = self.clients.write();

        for addr in to_remove {
            clients.remove(&addr);
        }

        for (addr, client) in to_add_clients {
            clients.insert(addr, client);
        }

        Ok(())
    }
}
