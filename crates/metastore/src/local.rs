use crate::errors::{MetastoreError, Result};
use crate::srv::Service;
use object_store::local::LocalFileSystem;
use object_store::{memory::InMemory, ObjectStore};
use protogen::metastore::gen::service::metastore_service_client::MetastoreServiceClient;
use protogen::metastore::gen::service::metastore_service_server::MetastoreServiceServer;
use std::path::Path;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tracing::info;

/// Starts an in-process, in-memory metastore.
pub async fn start_inprocess_inmemory() -> Result<MetastoreServiceClient<Channel>> {
    info!("starting in-process metastore");
    start_inprocess(Arc::new(InMemory::new())).await
}

/// Starts an in-process, local persistent metastore.
pub async fn start_inprocess_local(
    path: impl AsRef<Path>,
) -> Result<MetastoreServiceClient<Channel>> {
    let path = path.as_ref();
    info!(?path, "starting local metastore");
    let local = LocalFileSystem::new_with_prefix(path)?;
    start_inprocess(Arc::new(local)).await
}

/// Starts an in-process metastore service, returning a client for the service.
///
/// Useful for some tests, as well as when running GlareDB locally for testing.
/// This should never be used in production.
pub async fn start_inprocess(
    store: Arc<dyn ObjectStore>,
) -> Result<MetastoreServiceClient<Channel>> {
    let (client, server) = tokio::io::duplex(1024);

    tokio::spawn(async move {
        Server::builder()
            .add_service(MetastoreServiceServer::new(Service::new(store)))
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, MetastoreError>(server)]))
            .await
            .unwrap()
    });

    let mut client = Some(client);
    // Note that while we're providing a uri to bind to, we don't actually use
    // it.
    let channel = Endpoint::try_from("http://[::]/6545")
        .map_err(|e| MetastoreError::FailedInProcessStartup(format!("create endpoint: {}", e)))?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let client = client.take();
            async move {
                match client {
                    Some(client) => Ok(client),
                    None => Err(MetastoreError::FailedInProcessStartup(
                        "client already taken".to_string(),
                    )),
                }
            }
        }))
        .await
        .map_err(|e| {
            MetastoreError::FailedInProcessStartup(format!("connect with connector: {}", e))
        })?;

    Ok(MetastoreServiceClient::new(channel))
}
