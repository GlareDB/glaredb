use anyhow::{anyhow, Result};
use protogen::gen::rpcsrv::service::execution_service_server::ExecutionServiceServer;
use proxyutil::cloudauth::CloudAuthenticator;
use rpcsrv::proxy::RpcProxyHandler;
use std::net::SocketAddr;
use tonic::transport::{Identity, Server, ServerTlsConfig};
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

    pub async fn serve(
        self,
        addr: SocketAddr,
        rpc_server_cert: Option<String>,
        rpc_server_key: Option<String>,
    ) -> Result<()> {
        info!("starting rpc proxy service");

        // Note that we don't need a shutdown handler to prevent exits on active
        // connections. GRPC works over multiple connections, so the client
        // would just retry if the connection goes away.
        //
        // This _may_ end up killing inflight queries, but we can handle that
        // later.

        let mut server = Server::builder().trace_fn(|_| debug_span!("rpc_proxy_service_request"));

        match (rpc_server_cert, rpc_server_key) {
            (Some(cert), Some(key)) => {
                let cert = std::fs::read_to_string(cert)?;
                let key = std::fs::read_to_string(key)?;
                let identity = Identity::from_pem(cert, key);
                let tls_conf = ServerTlsConfig::new().identity(identity);
                server
                    .tls_config(tls_conf)?
                    .add_service(ExecutionServiceServer::new(self.handler))
                    .serve(addr)
                    .await?;
            }
            (None, None) => {
                server
                    .add_service(ExecutionServiceServer::new(self.handler))
                    .serve(addr)
                    .await?
            }
            _ => {
                return Err(anyhow!(
                    "both or neither of the server key and cert must be provided"
                ))
            }
        };

        Ok(())
    }
}
