use std::{net::SocketAddr, path::Path, sync::Arc};
use tonic::transport::Server;

use super::{network::ConsensusNetwork, store::ConsensusStore};
use crate::repr::{NodeId, Raft};
use crate::rpc::glaredb::GlaredbRpcHandler;
use crate::rpc::pb::raft_network_server::RaftNetworkServer;
use crate::rpc::pb::raft_node_server::RaftNodeServer;
use crate::rpc::pb::remote_data_source_server::RemoteDataSourceServer;
use crate::rpc::{ManagementRpcHandler, RaftRpcHandler};

pub mod app;

use app::ApplicationState;

pub async fn start_raft_node<P>(
    node_id: NodeId,
    dir: P,
    address: String,
    socket_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>>
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

    // TODO: derive address from socket_addr
    let app = Arc::new(ApplicationState {
        id: node_id,
        raft,
        address,
        store,
    });

    let raft_service = RaftNetworkServer::new(RaftRpcHandler::new(app.clone()));
    let node_handler = RaftNodeServer::new(ManagementRpcHandler::new(app.clone()));
    let glaredb_handler = RemoteDataSourceServer::new(GlaredbRpcHandler::new(app.clone()));

    Server::builder()
        .add_service(raft_service)
        .add_service(node_handler)
        .add_service(glaredb_handler)
        .serve(socket_addr)
        .await?;

    Ok(())
}
