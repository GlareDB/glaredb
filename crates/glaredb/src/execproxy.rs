use anyhow::Result;
use sqlexec::remoteexec::{proto::plan::plan_service_server::PlanServiceServer, proxy::ExecProxy};
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::{debug, debug_span};

pub struct RemoteExecProxy {
    proxy: ExecProxy,
}

impl RemoteExecProxy {
    pub fn new(api_addr: String, auth_code: String) -> Result<Self> {
        Ok(RemoteExecProxy {
            proxy: ExecProxy::new(api_addr, auth_code),
        })
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        debug!(%addr, "starting remote exec proxy");
        Server::builder()
            .trace_fn(|_| debug_span!("remote_exec_request"))
            .add_service(PlanServiceServer::new(self.proxy))
            .serve(addr)
            .await?;
        Ok(())
    }
}
