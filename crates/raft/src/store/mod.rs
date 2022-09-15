use std::{collections::BTreeMap, fmt::Debug, io::Cursor, ops::RangeBounds, sync::Arc};

use async_trait::async_trait;
use openraft::{
    storage::LogState, AnyError, EntryPayload, ErrorSubject, ErrorVerb, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, StorageIOError,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::trace;

use self::state::ConsensusStateMachine;

use super::message::Response;
use crate::{
    openraft_types::types::{
        EffectiveMembership, Entry, LogId, Snapshot, SnapshotMeta, StateMachineChanges,
        StorageError, Vote,
    },
    repr::RaftTypeConfig,
};

mod state;

#[derive(Debug, Default)]
pub struct ConsensusStore {
    last_purged_log_id: RwLock<Option<LogId>>,
    /// the raft log
    log: RwLock<BTreeMap<u64, Entry>>,

    pub state_machine: RwLock<ConsensusStateMachine>,

    /// current granted vote
    vote: RwLock<Option<Vote>>,
    snapshot_idx: Arc<Mutex<u64>>,
    current_snapshot: RwLock<Option<ConsensusSnapshot>>,
}
type StorageResult<T> = Result<T, StorageError>;

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusSnapshot {
    pub meta: SnapshotMeta,

    /// the data of the state machine at the time of this snapshot
    pub data: Vec<u8>,
}

/// Utility macro to return an io error
#[macro_export]
macro_rules! io_err {
    ($e:ident) => {
        new_io_err(ErrorSubject::Logs, ErrorVerb::Write, &$e)
    };
}

#[async_trait]
impl RaftStorage<RaftTypeConfig> for Arc<ConsensusStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        let mut v = self.vote.write().await;
        *v = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        Ok(*self.vote.read().await)
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&mut self, entries: &[&Entry]) -> StorageResult<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, (*entry).clone());
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId) -> StorageResult<()> {
        tracing::debug!("delete_logs_since: [{:?}, +oo)", log_id);

        let mut log = self.log.write().await;
        let keys = log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            log.remove(&key);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId) -> StorageResult<()> {
        tracing::debug!("purge_logs_upto: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let mut log = self.log.write().await;

            let keys = log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, EffectiveMembership), StorageError> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry],
    ) -> Result<Vec<Response>, StorageError> {
        trace!("apply_to_state_machine: {:?}", entries);
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response::None),
                // TODO: handle messages for the application in 'Normal'
                EntryPayload::Normal(ref _req) => todo!(),
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = EffectiveMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response::None)
                }
            };
        }

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Self::SnapshotData>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges, StorageError> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = ConsensusSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // update the state machine
        {
            let updated_state_machine: ConsensusStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;

            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // install current snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<Self::SnapshotData>>, StorageError> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[async_trait]
impl RaftLogReader<RaftTypeConfig> for Arc<ConsensusStore> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<RaftTypeConfig>> {
        let log = self.log.read().await;
        let last = log.iter().rev().next().map(|(_, ent)| ent.log_id);

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry>> {
        let log = self.log.read().await;
        let response = log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect();
        Ok(response)
    }
}

#[async_trait]
impl RaftSnapshotBuilder<RaftTypeConfig, Cursor<Vec<u8>>> for Arc<ConsensusStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<Cursor<Vec<u8>>>, StorageError> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // serialize the data of the state machine
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
        }

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().await;
            *l += 1;
            *l
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = ConsensusSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
