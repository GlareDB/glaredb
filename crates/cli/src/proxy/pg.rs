use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use pgsrv::errors::PgSrvError;
use pgsrv::proxy::ProxyHandler;
use pgsrv::ssl::SslConfig;
use proxyutil::cloudauth::CloudAuthenticator;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use tracing::{error, info, trace};

pub struct PgProxy {
    handler: Arc<ProxyHandler<CloudAuthenticator>>,
}

impl PgProxy {
    pub async fn new(
        api_addr: String,
        auth_code: String,
        ssl_server_cert: Option<String>,
        ssl_server_key: Option<String>,
    ) -> Result<Self> {
        let ssl_conf = match (ssl_server_cert, ssl_server_key) {
            (Some(cert), Some(key)) => Some(SslConfig::new(cert, key).await?),
            (None, None) => None,
            _ => {
                return Err(anyhow!(
                    "both or neither of the server key and cert must be provided"
                ))
            }
        };

        let auth = CloudAuthenticator::new(api_addr, auth_code)?;
        Ok(PgProxy {
            handler: Arc::new(ProxyHandler::new(auth, ssl_conf)),
        })
    }

    /// Start proxying connections from the given listener to the server.
    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        let conn_count = Arc::new(AtomicU64::new(0));

        // Shutdown handler.
        let (tx, mut rx) = oneshot::channel();
        let shutdown_conn_count = conn_count.clone();
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("shutdown triggered");
                    loop {
                        let conn_count = shutdown_conn_count.load(Ordering::Relaxed);
                        if conn_count == 0 {
                            // Shutdown!
                            let _ = tx.send(());
                            return;
                        }

                        info!(%conn_count, "shutdown prevented, active connections");

                        // Still have connections. Keep looping with some sleep in
                        // between.
                        tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
                    }
                }
                Err(err) => {
                    error!(%err, "unable to listen for shutdown signal");
                }
            }
        });

        loop {
            tokio::select! {
                _ = &mut rx => {
                    info!("shutting down");
                    return Ok(())
                }

                result = listener.accept() => {
                    let (inbound,_) = result?;
                    conn_count.fetch_add(1, Ordering::Relaxed);

                    let handler = self.handler.clone();
                    let conn_count = conn_count.clone();
                    tokio::spawn(async move {
                        trace!("client connected (proxy)");
                        match handler.proxy_connection(inbound).await {
                            Ok(_) => trace!("client disconnected from normal closure"),
                            // When we attempt to read the startup message from
                            // a closed connection, we will get an unexpected
                            // eof error.
                            //
                            // k8s tcp liveness checks will open and immediately
                            // close a connection which will trigger this case.
                            Err(PgSrvError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof =>
                                trace!("client disconnected from client-side close"),
                            Err(e) => error!(%e, "client disconnected with error"),
                        }
                        conn_count.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            }
        }
    }
}
