use anyhow::Result;
use proxyutil::cloudauth::CloudAuthenticator;
use rpcsrv::{
    export::arrow_flight::flight_service_client::FlightServiceClient,
    flight::handler::FlightServiceServer, proxy::ProxyHandler,
};
use std::net::SocketAddr;
use tonic::transport::{Channel, Identity, Server, ServerTlsConfig};
use tracing::{debug_span, info, warn};

use super::TLSMode;

// These paths exist as volume mounts on the cloud container running rpc proxy.
//
// TODO: Improve DX experience (for example, via ENV)
const CERT_PATH: &str = "/Users/corygrinstead/Development/glaredb/server-cert.pem";
const CERT_KEY_PATH: &str = "/Users/corygrinstead/Development/glaredb/server-key.pem";
type FlightProxyHandler = ProxyHandler<CloudAuthenticator, FlightServiceClient<Channel>>;

pub struct FlightProxy {
    handler: FlightProxyHandler,
}

impl FlightProxy {
    pub async fn new(api_addr: String, auth_code: String) -> Result<Self> {
        let auth = CloudAuthenticator::new(api_addr, auth_code)?;
        Ok(FlightProxy {
            handler: FlightProxyHandler::new(auth),
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

                todo!()
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

                todo!()
            }
        }

        Ok(())
    }
}

fn auth_interceptor() -> impl tonic::Interceptor {
    todo!()
}