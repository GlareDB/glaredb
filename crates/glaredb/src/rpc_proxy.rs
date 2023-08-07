use anyhow::Result;
use protogen::gen::rpcsrv::service::execution_service_server::ExecutionServiceServer;
use proxyutil::cloudauth::CloudAuthenticator;
use rpcsrv::proxy::RpcProxyHandler;
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::{debug_span, info};

pub struct RpcProxy {
    handler: RpcProxyHandler<CloudAuthenticator>,
}

impl RpcProxy {
    pub async fn new(api_addr: String, auth_code: String) -> Result<Self> {
        let auth = CloudAuthenticator::new(api_addr, auth_code)?;
        Ok(RpcProxy {
            handler: RpcProxyHandler::new(auth),
        })
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        info!("starting rpc proxy service");

        // Note that we don't need a shutdown handler to prevent exits on active
        // connections. GRPC works over multiple connections, so the client
        // would just retry if the connection goes away.
        //
        // This _may_ end up killing inflight queries, but we can handle that
        // later.

        Server::builder()
            .trace_fn(|_| debug_span!("rpc_proxy_service_request"))
            .add_service(ExecutionServiceServer::new(self.handler))
            .serve(addr)
            .await?;
        Ok(())
    }
}
