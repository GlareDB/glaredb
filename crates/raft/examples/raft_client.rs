//! Execute a query string against an in-memory data source.
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::Deref,
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, Result};
use maplit::btreeset;
use sqlengine::engine::Engine;

use raft::{
    client::ConsensusClient, lemur_impl::RaftClientSource, repr::NodeId,
    rpc::pb::AddLearnerRequest, server::start_raft_node,
};
use tokio::time::sleep;

async fn start_cluster() -> Result<()> {
    let d1 = tempdir::TempDir::new("test_cluster")?;
    let d2 = tempdir::TempDir::new("test_cluster")?;
    let d3 = tempdir::TempDir::new("test_cluster")?;

    let _h1 = tokio::spawn(async move {
        let addr = get_rpc_addr(1);
        let url = get_rpc_str(1);
        let x = start_raft_node(1, d1.path(), url, addr).await;
        println!("x: {:?}", x);
    });

    let _h2 = tokio::spawn(async move {
        let addr = get_rpc_addr(2);
        let url = get_rpc_str(2);
        let x = start_raft_node(2, d2.path(), url, addr).await;
        println!("x: {:?}", x);
    });

    let _h3 = tokio::spawn(async move {
        let addr = get_rpc_addr(3);
        let url = get_rpc_str(3);
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

    let query = std::env::args().nth(3).ok_or("expected query argument")?;

    let client = ConsensusClient::new(1, get_rpc_str(1));

    let source = RaftClientSource::from_client(client);
    let mut engine = Engine::new(source);
    engine.ensure_system_tables().await?;
    let mut session = engine.begin_session()?;

    let results = session.execute_query(&query).await?;
    for result in results.into_iter() {
        println!("{:#?}", result);
    }

    Ok(())
}

fn get_rpc_addr(node_id: u32) -> SocketAddr {
    let addr = match node_id {
        1 => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6001),
        2 => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6002),
        3 => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6003),
        _ => panic!("node not found"),
    };
    addr
}

fn get_rpc_str(node_id: u32) -> String {
    let addr = get_rpc_addr(node_id);
    format!("http://{}", addr)
}
