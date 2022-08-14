use crate::rocks::RocksStore;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use openraft::{
    storage::{LogState, Snapshot},
    BasicNode, EffectiveMembership, Entry, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    RaftTypeConfig, SnapshotMeta, StateMachineChanges, StorageError, Vote,
};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

pub type NodeId = u64;

/// A request that maps directly to methods of the underlying store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageRequest {}

/// A request related to cluster and raft managment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManagementRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Storage(StorageRequest),
    Management(ManagementRequest),
}

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

/// A node in the cluster.
#[derive(Debug, Clone)]
pub struct Node {
    id: NodeId,
    /// Storage for raft related stuff.
    raft: Arc<DB>,
    /// State that will be modified.
    state: RocksStore, // TODO: Have some sort of "Store" interface here instead.
}

impl Node {
    pub fn new(id: NodeId, raft: Arc<DB>, state: RocksStore) -> Node {
        Node { id, raft, state }
    }
}

#[async_trait]
impl RaftLogReader<TypeConfig> for Node {
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        unimplemented!()
    }

    async fn try_get_log_entries<R>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        R: RangeBounds<u64> + Clone + fmt::Debug + Send + Sync,
    {
        unimplemented!()
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig, Cursor<Vec<u8>>> for Node {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<NodeId, BasicNode, Cursor<Vec<u8>>>, StorageError<NodeId>> {
        unimplemented!()
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Node {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        unimplemented!()
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        unimplemented!()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        unimplemented!()
    }

    async fn append_to_log(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> Result<(), StorageError<NodeId>> {
        unimplemented!()
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        unimplemented!()
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        unimplemented!()
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            EffectiveMembership<NodeId, BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        unimplemented!()
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        unimplemented!()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<NodeId>> {
        unimplemented!()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<TypeConfig>, StorageError<NodeId>> {
        unimplemented!()
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NodeId, BasicNode, Self::SnapshotData>>, StorageError<NodeId>> {
        unimplemented!()
    }
}
