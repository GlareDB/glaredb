use std::{net::SocketAddr, path::Path, sync::Arc};
use tokio::{net::TcpListener, task};

use super::{
    app::ApplicationState, network::ConsensusNetwork, raft::Raft, store::ConsensusStore,
    GlareNodeId, GlareRaft,
};

pub async fn start_raft_node<P>(
    node_id: GlareNodeId,
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
    let raft = GlareRaft::new(node_id, config.clone(), network, store.clone());

    let app = Arc::new(ApplicationState { id: node_id, raft });

    let service = Arc::new(Raft::new(app.clone()));

    let server = toy_rpc::Server::builder().register(service).build();

    // Initialize RPC server
    let listener = TcpListener::bind(rpc_addr).await.unwrap();
    let rpc_handler = task::spawn(async move {
        server.accept(listener).await.unwrap();
    });

    // Initialize HTTP server

    // Run both tasks
    rpc_handler.await?;
    Ok(())
    /*
    let mut app: Server = tide::Server::with_state(app);

    management::rest(&mut app);
    api::rest(&mut app);

    app.listen(http_addr).await?;
    handle.await;
    Ok(())
    */
}
