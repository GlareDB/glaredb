use crate::rocks::RocksStore;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use openraft::{
    storage::{LogState, Snapshot},
    AnyError, BasicNode, DefensiveCheck, DefensiveError, EffectiveMembership, Entry, EntryPayload,
    ErrorSubject, ErrorVerb, LogId, Membership, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    RaftTypeConfig, SnapshotMeta, StateMachineChanges, StorageError, StorageIOError, Vote,
};
use rocksdb::{ColumnFamily, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::raft::{NodeId, Request, Response, TypeConfig};

const RAFT_META_CF: &str = "raft_meta";
const RAFT_LOGS_CF: &str = "raft_logs";

#[derive(Debug)]
struct AppliedState {
    /// The latest log to be applied.
    log: Option<LogId<NodeId>>,
    /// The latest membership to be applied.
    membership: EffectiveMembership<NodeId, BasicNode>,
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
    /// Create a new node, with the provided rocksdb and state.
    pub fn new(id: NodeId, raft: Arc<DB>, state: RocksStore) -> Result<Node> {
        // TODO: Where to create column families?
        Ok(Node { id, raft, state })
    }

    /// Apply all entries to the underlying state.
    ///
    /// Entries are assumed to ordered by index.
    fn apply(&self, entries: &[&Entry<TypeConfig>]) -> Result<Vec<Response>, StorageError<NodeId>> {
        let mut responses = Vec::with_capacity(entries.len());
        let cf = self.raft_meta_handle()?;
        for entry in entries.iter() {
            self.raft
                .put_cf(
                    cf,
                    b"last_applied",
                    bincode::serialize(&entry.log_id).map_err(|e| {
                        new_io_err(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, &e)
                    })?,
                )
                .map_err(|e| new_io_err(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, &e))?;

            match &entry.payload {
                EntryPayload::Blank => (),
                EntryPayload::Normal(request) => {
                    // TODO: Handle me
                }
                EntryPayload::Membership(membership) => {
                    let effective =
                        EffectiveMembership::new(Some(entry.log_id), membership.clone());
                    let buf = bincode::serialize(&effective).map_err(|e| {
                        new_io_err(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, &e)
                    })?;
                    self.raft.put_cf(cf, b"membership", &buf).map_err(|e| {
                        new_io_err(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, &e)
                    })?;
                }
            }
        }

        Ok(responses)
    }

    /// Get the last applied state.
    fn applied_state(&self) -> Result<AppliedState, StorageError<NodeId>> {
        let cf = self.raft_meta_handle()?;
        let log = {
            let buf = self
                .raft
                .get_pinned_cf(cf, b"last_applied")
                .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Read, &e))?;
            match buf {
                Some(buf) => Some(
                    bincode::deserialize(&buf)
                        .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Read, &e))?,
                ),
                None => None,
            }
        };
        let membership = {
            let buf = self
                .raft
                .get_pinned_cf(cf, b"membership")
                .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Read, &e))?;
            match buf {
                Some(buf) => bincode::deserialize(&buf)
                    .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Read, &e))?,
                None => EffectiveMembership::default(),
            }
        };

        Ok(AppliedState { log, membership })
    }

    fn raft_meta_handle(&self) -> Result<&ColumnFamily, StorageError<NodeId>> {
        self.raft.cf_handle(RAFT_META_CF).ok_or(
            StorageIOError::new(
                ErrorSubject::Store,
                ErrorVerb::Read,
                AnyError::error("failed to get raft meta cf"),
            )
            .into(),
        )
    }

    fn raft_logs_handle(&self) -> Result<&ColumnFamily, StorageError<NodeId>> {
        self.raft.cf_handle(RAFT_META_CF).ok_or(
            StorageIOError::new(
                ErrorSubject::Store,
                ErrorVerb::Read,
                AnyError::error("failed to get raft logs cf"),
            )
            .into(),
        )
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
        let vote_buf = bincode::serialize(vote)
            .map_err(|e| new_io_err(ErrorSubject::Vote, ErrorVerb::Write, &e))?;
        self.raft
            .put_cf(self.raft_meta_handle()?, b"vote", &vote_buf)
            .map_err(|e| new_io_err(ErrorSubject::Vote, ErrorVerb::Write, &e))?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self
            .raft
            .get_pinned_cf(self.raft_meta_handle()?, b"vote")
            .map_err(|e| new_io_err(ErrorSubject::Vote, ErrorVerb::Read, &e))?
            .and_then(|buf| bincode::deserialize(&buf).ok()))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> Result<(), StorageError<NodeId>> {
        let mut batch = WriteBatch::default();
        let cf = self.raft_logs_handle()?;
        for entry in entries.iter() {
            let key = &entry_key(&entry.log_id);
            let buf = bincode::serialize(entry)
                .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Write, &e))?;
            batch.put_cf(cf, &key, &buf);
        }

        self.raft
            .write(batch)
            .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Write, &e))?;

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let cf = self.raft_logs_handle()?;
        self.raft
            .delete_range_cf(cf, entry_key(&log_id), max_entry_key())
            .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Delete, &e))?;
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.raft_logs_handle()?;
        self.raft
            .delete_range_cf(cf, min_entry_key(), entry_key(&log_id))
            .map_err(|e| new_io_err(ErrorSubject::Logs, ErrorVerb::Delete, &e))?;
        Ok(())
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
        let state = self.applied_state()?;
        Ok((state.log, state.membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        // TODO: Does the number of responses need to equal the number of entries?
        self.apply(entries)
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

/// Encode a log entry's index as an order-preserving key.
const fn entry_key(id: &LogId<NodeId>) -> [u8; 8] {
    u64::to_be_bytes(id.index)
}

const fn min_entry_key() -> [u8; 8] {
    [0; 8]
}

const fn max_entry_key() -> [u8; 8] {
    [255; 8]
}

/// Simple wrapper around creating a `StorageError` with the IO variant.
fn new_io_err<E, N>(subject: ErrorSubject<N>, verb: ErrorVerb, error: &E) -> StorageError<N>
where
    E: std::error::Error + 'static,
    N: openraft::NodeId,
{
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::new(error)),
    }
}
