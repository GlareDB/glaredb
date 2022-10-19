use object_store_util::temp::TempObjectStore;
use pgsrv::handler::{Handler, PostgresHandler};
use sqlexec::engine::Engine;
use sqlexec::runtime::AccessRuntime;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logutil::init(1);

    let args: Vec<_> = std::env::args().collect();
    let bind_addr = args.get(1).cloned().unwrap_or("localhost:0".to_string());

    let listener = TcpListener::bind(bind_addr).await?;
    let listen_addr = listener.local_addr()?;
    info!(%listen_addr, "listening");

    let engine = Engine::new(
        "test",
        Arc::new(AccessRuntime::new(Arc::new(TempObjectStore::new()?))),
    )?;

    let handler = Arc::new(Handler::new(engine));

    loop {
        let (socket, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            match handler.handle_connection(socket).await {
                Ok(_) => info!("connection exiting"),
                Err(e) => error!(%e, "failed to handle connection"),
            };
        });
    }
}
