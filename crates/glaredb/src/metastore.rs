use anyhow::Result;
use metastore::srv::Service;
use metastore_client::proto::service::metastore_service_server::MetastoreServiceServer;
use object_store::ObjectStore;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{debug, debug_span};

pub struct Metastore {
    service: Service,
}

impl Metastore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Result<Self> {
        Ok(Metastore {
            service: Service::new(store),
        })
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        debug!(%addr, "starting metastore service");
        Server::builder()
            .trace_fn(|_| debug_span!("metastore_service_request"))
            .add_service(MetastoreServiceServer::new(self.service))
            .serve(addr)
            .await?;
        Ok(())
    }
}
