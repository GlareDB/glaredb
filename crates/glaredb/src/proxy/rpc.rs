use anyhow::Result;
use clap::ValueEnum;
use protogen::gen::rpcsrv::service::execution_service_server::ExecutionServiceServer;
use proxyutil::cloudauth::CloudAuthenticator;
use rpcsrv::{
    flight::{handler::FlightServiceServer, proxy::CloudFlightProxyHandler},
    proxy::CloudRpcProxyHandler,
};
use std::net::SocketAddr;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{debug_span, info, warn};

pub struct RpcProxy {
    rpc_handler: CloudRpcProxyHandler,
    flight_handler: CloudFlightProxyHandler,
    tls_cert_path: String,
    tls_key_path: String,
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
    pub async fn new(
        api_addr: String,
        auth_code: String,
        tls_cert_path: String,
        tls_key_path: String,
    ) -> Result<Self> {
        let auth = CloudAuthenticator::new(api_addr, auth_code)?;
        Ok(RpcProxy {
            rpc_handler: CloudRpcProxyHandler::new(auth.clone()),
            flight_handler: CloudFlightProxyHandler::new(auth),
            tls_cert_path,
            tls_key_path,
        })
    }

    pub async fn serve(self, addr: SocketAddr, tls_opts: TLSMode) -> Result<()> {
        info!("starting rpc proxy service");
        let mut server = Server::builder().trace_fn(|_| debug_span!("rpc_proxy_service_request"));

        match tls_opts {
            TLSMode::Disabled => {
                warn!("TLS is disabled for RPC service");

                server
                    .add_service(ExecutionServiceServer::new(self.rpc_handler))
                    .add_service(FlightServiceServer::new(self.flight_handler))
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
                    TLSMode::Disabled => unreachable!("impossible TLS option"),
                };

                let cert = std::fs::read_to_string(self.tls_cert_path)?;
                let key = std::fs::read_to_string(self.tls_key_path)?;

                let identity = Identity::from_pem(cert, key);

                server
                    .tls_config(
                        ServerTlsConfig::new()
                            .identity(identity)
                            .client_auth_optional(tls_optional),
                    )?
                    .add_service(ExecutionServiceServer::new(self.rpc_handler))
                    .add_service(FlightServiceServer::new(self.flight_handler))
                    .serve(addr)
                    .await?;
            }
        }

        Ok(())
    }
}
