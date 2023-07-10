use crate::remoteexec::errors::{RemoteExecError, Result};
use crate::remoteexec::proto::plan::plan_service_client::PlanServiceClient;
use crate::remoteexec::proto::plan::plan_service_server::PlanService;
use crate::remoteexec::proto::plan::{ExecutePlanRequest, ExecutePlanResponse};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

/// Proxy plans to some remote server, authentication connections with Cloud.
pub struct ExecProxy {
    api_url: String,
    cloud_client: reqwest::Client,

    /// Map of clients.
    ///
    /// Keyed by address we get back from Cloud.
    clients: Arc<RwLock<BTreeMap<String, PlanServiceClient<Channel>>>>,
}

impl ExecProxy {
    pub fn new() -> ExecProxy {
        unimplemented!()
        // ExecProxy {
        //     clients: Arc::new(RwLock::new(BTreeMap::new())),
        // }
    }

    async fn authenticate(&self) -> Result<DatabaseDetails> {
        let query = [
            ("user", "user"),
            // ("password", password),
            // ("name", db_name),
            // ("orgname", org),
            // ("compute_engine", compute_engine),
        ];

        let res = self
            .cloud_client
            .get(format!(
                "{}/api/internal/databases/authenticate",
                &self.api_url
            ))
            .query(&query)
            .send()
            .await?;

        // Currently only expect '200' from the cloud service. For
        // anything else, return an erorr.
        //
        // Does not try to deserialize the error responses to allow for
        // flexibility and changes on the cloud side during initial
        // development.
        if res.status().as_u16() != 200 {
            let text = res.text().await?;
            return Err(RemoteExecError::CloudResponse(text));
        }

        let db_details: DatabaseDetails = res.json().await?;

        Ok(db_details)
    }
}

#[async_trait]
impl PlanService for ExecProxy {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let details = self.authenticate().await?;

        let maybe_client = {
            let clients = self.clients.read();
            clients.get(&details.ip).cloned()
        };

        let mut client = match maybe_client {
            Some(client) => client,
            None => {
                let client = PlanServiceClient::connect(format!("{}:{}", details.ip, details.port))
                    .await
                    .map_err(|e| tonic::Status::from_error(Box::new(e)))?;
                let mut clients = self.clients.write();
                clients.insert(details.ip, client.clone());
                client
            }
        };

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            // TODO: No unwrap
            let mut stream = client.execute_plan(request).await.unwrap().into_inner();

            while let Some(item) = stream.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ExecutePlanStream
        ))
    }
}

#[derive(Deserialize, Debug, Clone)]
struct DatabaseDetails {
    /// IP to connect to.
    // TODO: Rename to host.
    ip: String,
    /// Port to connect to.
    port: String,
    /// ID of the database we're connecting to (UUID).
    database_id: String,
    /// ID of the user initiating the connection (UUID).
    user_id: String,
    /// Bucket for session storage.
    gcs_storage_bucket: String,
    /// Max number of data sources allowed
    max_datasource_count: usize,
    /// Memory limit applied to session in bytes
    memory_limit_bytes: usize,
    /// Max number of tunnels allowed
    max_tunnel_count: usize,
}
