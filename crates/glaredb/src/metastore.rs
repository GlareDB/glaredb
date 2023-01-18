use anyhow::Result;
use metastore::proto::service::metastore_service_server::MetastoreServiceServer;
use metastore::srv::Service;
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::{debug, debug_span};

pub struct Metastore {
    service: Service,
}

impl Metastore {
    pub fn new() -> Result<Self> {
        Ok(Metastore {
            service: Service::new(),
        })
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        debug!(%addr, "starting metastore service");
        Server::builder()
            .trace_fn(|_| debug_span!("metastore trace"))
            .add_service(MetastoreServiceServer::new(self.service))
            .serve(addr)
            .await?;
        Ok(())
    }
}
