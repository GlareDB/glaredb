//! Execute a query string against an in-memory data source.
use std::{net::{SocketAddr, IpAddr, Ipv4Addr}, str::FromStr, ops::Deref, time::Duration};

use anyhow::{Result, anyhow};
use maplit::btreeset;
use sqlengine::engine::Engine;

use raft::{lemur_impl::RaftClientSource, repr::NodeId, server::start_raft_node, client::ConsensusClient, rpc::pb::AddLearnerRequest};
use tokio::time::sleep;

async fn start_cluster() -> Result<()> {
    let d1 = tempdir::TempDir::new("test_cluster")?;
    let d2 = tempdir::TempDir::new("test_cluster")?;
    let d3 = tempdir::TempDir::new("test_cluster")?;

    let _h1 = tokio::spawn(async move {
        let addr = get_rpc_addr(1);
        let url = format!("http://{}", addr);
        let x = start_raft_node(1, d1.path(), url, addr).await;
        println!("x: {:?}", x);
    });

    let _h2 = tokio::spawn(async move {
        let addr = get_rpc_addr(2);
        let url = format!("http://{}", addr);
        let x = start_raft_node(2, d2.path(), url, addr).await;
        println!("x: {:?}", x);
    });

    let _h3 = tokio::spawn(async move {
        let addr = get_rpc_addr(3);
        let url = format!("http://{}", addr);
        let x = start_raft_node(3, d3.path(), url, addr).await;
        println!("x: {:?}", x);
    });

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logutil::init(1);

    // let leader_id = std::env::args().nth(1).ok_or("expected leader_id argument")?;
    // let leader_addr = std::env::args().nth(2).ok_or("expected leader_addr argument")?;
    let leader_id = 1;
    let leader_addr = get_rpc_addr(leader_id);

    let query = std::env::args().nth(3).ok_or("expected query argument")?;

    tokio::spawn(async move {
        start_cluster().await.unwrap();
    });
    
    // Initiate the leader
    let url = format!("http://{}", get_rpc_addr(1));
    let client = ConsensusClient::new(1, url);
    client.init().await.expect("failed to init cluster");
    client
        .add_learner(AddLearnerRequest {
            node_id: 2,
            address: get_rpc_addr(2).to_string(),
        })
        .await?;

    client
        .add_learner(AddLearnerRequest {
            node_id: 3,
            address: get_rpc_addr(3).to_string(),
        })
        .await?;
    let _x = client.change_membership(&btreeset! {1,2,3}).await?;
    let metrics = client.metrics().await?;
    println!("printed metrics {:?}", metrics);


    sleep(Duration::from_millis(10000)).await;

    let source = RaftClientSource::from_client(client);
    let mut engine = Engine::new(source);
    println!("ensuring tables");
    engine.ensure_system_tables().await?;
    println!("begining session");
    let mut session = engine.begin_session()?;

    let results = session.execute_query(&query).await?;
    for result in results.into_iter() {
        println!("{:#?}", result);
    }

    Ok(())
}

fn get_rpc_addr(node_id: u32) -> SocketAddr {
    let addr = match node_id {
        1 => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 22001),
        2 => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 22002),
        3 => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 22003),
        _ => panic!("node not found"),
    };
    addr
}

fn get_rpc_str(node_id: u32) -> String {
    let addr = get_rpc_addr(node_id);
    addr.to_string()
}
