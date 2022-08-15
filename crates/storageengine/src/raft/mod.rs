use openraft::{RaftTypeConfig}
use serde::{Serialize, Deserialize};

pub mod network;
pub mod node;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {}

#[derive(Debug, Clone, Copy, Default, PartialEq, PartialOrd, Eq, Ord)]
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = Request;
    type R = Response;
    type NodeId = NodeId;
    type Node = BasicNode;
}
