use anyhow::Result;
use protogen::gen::rpcsrv::service::execution_service_server::ExecutionServiceServer;
use proxyutil::cloudauth::CloudAuthenticator;
use rpcsrv::proxy::RpcProxyHandler;
use std::net::SocketAddr;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{debug_span, info};

// These paths exist as volume mounts on the cloud container running rpc proxy.
//
// TODO: Improve DX experience (for example, via ENV)
const CERT_PATH: &str = "/etc/certs/tls.crt";
const CERT_KEY_PATH: &str = "/etc/certs/tls.key";

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

    pub async fn serve(self, addr: SocketAddr, disable_tls: bool) -> Result<()> {
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
        } else {
            info!("applying TLS to rpc service");

            let cert = std::fs::read_to_string(CERT_PATH)?;
            let key = std::fs::read_to_string(CERT_KEY_PATH)?;

            let identity = Identity::from_pem(cert, key);

            server
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(identity)
                        .client_auth_optional(true),
                )?
                .add_service(ExecutionServiceServer::new(self.handler))
                .serve(addr)
                .await?;
        }

        Ok(())
    }
}
