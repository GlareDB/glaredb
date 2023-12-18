use anyhow::Result;
use clap::ValueEnum;
use protogen::gen::rpcsrv::service::{
    execution_service_client::ExecutionServiceClient,
    execution_service_server::ExecutionServiceServer,
};
use proxyutil::cloudauth::CloudAuthenticator;
use rpcsrv::proxy::ProxyHandler;
use std::net::SocketAddr;
use tonic::{transport::{Channel, Identity, Server, ServerTlsConfig}, service::Interceptor};
use tracing::{debug_span, info, warn};

// These paths exist as volume mounts on the cloud container running rpc proxy.
//
// TODO: Improve DX experience (for example, via ENV)
const CERT_PATH: &str = "/etc/certs/tls.crt";
const CERT_KEY_PATH: &str = "/etc/certs/tls.key";

type RpcProxyHandler = ProxyHandler<CloudAuthenticator, ExecutionServiceClient<Channel>>;

pub struct RpcProxy {
    handler: RpcProxyHandler,
}

#[derive(Clone, Debug, Default, ValueEnum)]
#[clap(rename_all = "snake")]
pub enum TLSMode {
    #[default]
    Required,
    SkipVerify,
    Disabled,
}

impl RpcProxy {
    pub async fn new(api_addr: String, auth_code: String) -> Result<Self> {
        let auth = CloudAuthenticator::new(api_addr, auth_code)?;
        Ok(RpcProxy {
            handler: RpcProxyHandler::new(auth),
        })
    }

    pub async fn serve(self, addr: SocketAddr, tls_opts: TLSMode) -> Result<()> {
        info!("starting rpc proxy service");

        // Note that we don't need a shutdown handler to prevent exits on active
        // connections. GRPC works over multiple connections, so the client
        // would just retry if the connection goes away.
        //
        // This _may_ end up killing inflight queries, but we can handle that
        // later.

        let mut server = Server::builder().trace_fn(|_| debug_span!("rpc_proxy_service_request"));

        match tls_opts {
            TLSMode::Disabled => {
                warn!("TLS is disabled for RPC service");

                server
                    .add_service(ExecutionServiceServer::new(self.handler))
                    .serve(addr)
                    .await?
            }
            TLSMode::Required | TLSMode::SkipVerify => {
                let tls_optional = match tls_opts {
                    TLSMode::Required => {
                        info!("TLS is enabled for RPC service");
                        false
                    }
                    TLSMode::SkipVerify => {
                        warn!("TLS is optional for RPC service");
                        true
                    }
                    TLSMode::Disabled => panic!("impossible TLS option"),
                };

                let cert = std::fs::read_to_string(CERT_PATH)?;
                let key = std::fs::read_to_string(CERT_KEY_PATH)?;

                let identity = Identity::from_pem(cert, key);
                // tonic::service::interceptor(intercept);
                server
                    .tls_config(
                        ServerTlsConfig::new()
                            .identity(identity)
                            .client_auth_optional(tls_optional),
                    ).unwrap()
                    .add_service(ExecutionServiceServer::new(self.handler))
                    .serve(addr)
                    .await?;
            }
        }

        Ok(())
    }
}

struct ProxyInterceptor;
impl Interceptor for ProxyInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> std::prelude::v1::Result<tonic::Request<()>, tonic::Status> {
        todo!()
    }
}