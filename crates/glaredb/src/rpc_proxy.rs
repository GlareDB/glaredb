use anyhow::{anyhow, Result};
use protogen::gen::rpcsrv::service::execution_service_server::ExecutionServiceServer;
use proxyutil::cloudauth::CloudAuthenticator;
use rpcsrv::proxy::RpcProxyHandler;
use std::net::SocketAddr;
use std::path::PathBuf;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{debug_span, info};

pub struct RpcProxy {
    handler: RpcProxyHandler<CloudAuthenticator>,
}

impl RpcProxy {
    pub async fn new(
        api_addr: String,
        auth_code: String,
        ca_domain: Option<String>,
    ) -> Result<Self> {
        let auth = CloudAuthenticator::new(api_addr, auth_code)?;
        Ok(RpcProxy {
            handler: RpcProxyHandler::new(auth, ca_domain),
        })
    }

    pub async fn serve(
        self,
        addr: SocketAddr,
        server_cert_path: Option<PathBuf>,
        server_key_path: Option<PathBuf>,
        disable_tls: bool,
    ) -> Result<()> {
        info!("starting rpc proxy service");

        // Note that we don't need a shutdown handler to prevent exits on active
        // connections. GRPC works over multiple connections, so the client
        // would just retry if the connection goes away.
        //
        // This _may_ end up killing inflight queries, but we can handle that
        // later.

        let mut server = Server::builder().trace_fn(|_| debug_span!("rpc_proxy_service_request"));

        if disable_tls {
            server
                .add_service(ExecutionServiceServer::new(self.handler))
                .serve(addr)
                .await?
        } else if let (Some(server_cert_path), Some(server_key_path)) =
            (server_cert_path, server_key_path)
        {
            let cert = std::fs::read_to_string(server_cert_path)?;
            let key = std::fs::read_to_string(server_key_path)?;
            let identity = Identity::from_pem(cert, key);
            let tls_conf = ServerTlsConfig::new().identity(identity);

            server
                .tls_config(tls_conf)?
                .add_service(ExecutionServiceServer::new(self.handler))
                .serve(addr)
                .await?;
        } else {
            return Err(anyhow!(
                "Specify server-cert-path and server-key-path in --args for TLS"
            ));
        }

        Ok(())
    }
}
