use std::{net::SocketAddr, path::Path, sync::Arc};
use tokio::{net::TcpListener, task};

use crate::repr::{NodeId, Raft};
use super::{
    app::ApplicationState, network::ConsensusNetwork, client::rpc::Raft as RaftRpc, store::ConsensusStore,
};

mod management;

pub type HttpServer = tide::Server<Arc<ApplicationState>>;
pub async fn start_raft_node<P>(
    node_id: NodeId,
    dir: P,
    rpc_addr: SocketAddr,
    http_addr: SocketAddr,
) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    // Create a configuration for the raft instance.
    let config = Arc::new(openraft::Config::default().validate().unwrap());

    // Create a instance of where the Raft data will be stored.
    let store = ConsensusStore::new(&dir).await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = Arc::new(ConsensusNetwork {});

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone());

    let app = Arc::new(ApplicationState {
        id: node_id,
        raft,
        api_addr: http_addr.to_string(),
        rpc_addr: rpc_addr.to_string(),
    });

    let service = Arc::new(RaftRpc::new(app.clone()));

    let server = toy_rpc::Server::builder().register(service).build();

    // Initialize RPC server
    let listener = TcpListener::bind(rpc_addr).await.unwrap();
    let rpc_handler = task::spawn(async move {
        server.accept(listener).await.unwrap();
    });

    // Initialize HTTP server
    let mut app: HttpServer = tide::Server::with_state(app);

    management::rest(&mut app);
    let http_handler = task::spawn(async move {
        app.listen(http_addr).await.unwrap();
    });
    // Run both tasks
    let (rpc_res, http_res) = futures::join!(rpc_handler, http_handler);
    rpc_res.unwrap();
    http_res.unwrap();
    Ok(())
}
