use std::{sync::Arc, io::Cursor, ops::RangeBounds, fmt::Debug, error::Error, collections::BTreeMap, path::Path};

use async_trait::async_trait;
use openraft::{RaftStorage, Vote, StorageError, Entry, LogId, EffectiveMembership, StateMachineChanges, storage::{Snapshot, LogState}, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, ErrorSubject, ErrorVerb, StorageIOError, AnyError, EntryPayload};
use rocksdb::{ColumnFamily};
use serde::{Serialize, Deserialize};
use tokio::sync::RwLock;

use super::{GlareTypeConfig, GlareNodeId, GlareNode, messaging::{GlareResponse, GlareRequest}};

pub struct ConsensusStore {
    db: Arc<rocksdb::DB>,

    pub state_machine: RwLock<ConsensusStateMachine>,
}
type StorageResult<T> = Result<T, StorageError<GlareNodeId>>;

fn id_to_bin(id: u64) -> Vec<u8> {
    bincode::serialize(&id).unwrap()
}

fn bin_to_id(buf: &[u8]) -> u64 {
    bincode::deserialize(buf).unwrap()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusSnapshot {
    pub meta: SnapshotMeta<GlareNodeId, GlareNode>,

    /// the data of the state machine at the time of this snapshot
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct ConsensusStateMachine {
    pub db: Arc<rocksdb::DB>,
}

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError<GlareNodeId> {
    StorageIOError::new(ErrorSubject::StateMachine, ErrorVerb::Read, AnyError::new(&e)).into()
}

fn sm_w_err<E: Error + 'static>(e: E) -> StorageError<GlareNodeId> {
    StorageIOError::new(ErrorSubject::StateMachine, ErrorVerb::Write, AnyError::new(&e)).into()
}

impl ConsensusStateMachine {
    fn get_last_membership(&self) -> StorageResult<EffectiveMembership<GlareNodeId, GlareNode>> {
        self.db
            .get_cf(
                self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_membership".as_bytes(),
            )
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .unwrap_or_else(|| Ok(EffectiveMembership::default()))
            })
    }

    fn set_last_membership(&self, membership: EffectiveMembership<GlareNodeId, GlareNode>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_membership".as_bytes(),
                serde_json::to_vec(&membership).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    fn get_last_applied_log(&self) -> StorageResult<Option<LogId<GlareNodeId>>> {
        self.db
            .get_cf(
                self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_applied_log".as_bytes(),
            )
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }

    fn set_last_applied_log(&self, log_id: LogId<GlareNodeId>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_applied_log".as_bytes(),
                serde_json::to_vec(&log_id).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    fn from_serializable(sm: SerializableConsensusStateMachine, db: Arc<rocksdb::DB>) -> StorageResult<Self> {
        for (key, value) in sm.data {
            db.put_cf(db.cf_handle("data").unwrap(), key.as_bytes(), value.as_bytes()).map_err(sm_w_err)?;
        }

        let r = Self { db };
        if let Some(log_id) = sm.last_applied_log {
            r.set_last_applied_log(log_id)?;
        }
        r.set_last_membership(sm.last_membership)?;

        Ok(r)
    }

    pub fn new(db: Arc<rocksdb::DB>) -> Self {
        Self { db }
    }

    fn insert(&self, key: String, value: String) -> StorageResult<()> {
        self.db
            .put_cf(self.db.cf_handle("data").unwrap(), key.as_bytes(), value.as_bytes())
            .map_err(|e| StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into())
    }

    pub fn get(&self, key: &str) -> StorageResult<Option<String>> {
        let key = key.as_bytes();
        self.db
            .get_cf(self.db.cf_handle("data").unwrap(), key)
            .map(|value| {
                value.map(|value| String::from_utf8(value.to_vec()).expect("invalid data"))
            })
            .map_err(|e| StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into())
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableConsensusStateMachine {
    pub last_applied_log: Option<LogId<GlareNodeId>>,
    pub last_membership: EffectiveMembership<GlareNodeId, GlareNode>,
    /// application data
    pub data: BTreeMap<String, String>,
}

impl From<&ConsensusStateMachine> for SerializableConsensusStateMachine {
    fn from(state: &ConsensusStateMachine) -> Self {
        let mut data = BTreeMap::new();

        for r in state.db.iterator_cf(
            state.db.cf_handle("data").expect("cf_handle"),
            rocksdb::IteratorMode::Start,
        ) {
            if let Ok((key, value)) = r {
                let key: &[u8] = &key;
                let value: &[u8] = &value;
                data.insert(
                    String::from_utf8(key.to_vec()).expect("invalid key"),
                    String::from_utf8(value.to_vec()).expect("invalid data"),
                );
            } else {
                panic!("iterator error");
            }
        }

        Self {
            last_applied_log: state.get_last_applied_log().unwrap(),
            last_membership: state.get_last_membership().unwrap(),
            data,
        }
    }
}

/// Utility macro to return an io error
#[macro_export]
macro_rules! io_err {
    ($e:ident) => {
        new_io_err(ErrorSubject::Logs, ErrorVerb::Write, &$e)
    };
}

impl ConsensusStore {
    fn store(&self) -> &ColumnFamily {
        self.db.cf_handle("store").unwrap()
    }

    fn logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    fn get_last_purged_(&self) -> StorageResult<Option<LogId<u64>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"last_purged_log_id")
            .map_err(|e| StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)))?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: LogId<u64>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.store(),
                b"last_purged_log_id",
                serde_json::to_vec(&log_id).unwrap().as_slice(),
            )
            .map_err(|e| StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into())
    }

    fn get_snapshot_index_(&self) -> StorageResult<u64> {
        Ok(self
            .db
            .get_cf(self.store(), b"snapshot_index")
            .map_err(|e| StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)))?
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or(0)
        )

    }

    fn set_snapshot_index_(&self, snapshot_index: u64) -> StorageResult<()> {
        self.db
            .put_cf(
                self.store(),
                b"snapshot_index",
                serde_json::to_vec(&snapshot_index).unwrap().as_slice(),
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)),
            })?;

        Ok(())
    }

    fn set_vote_(&self, vote: &Vote<GlareNodeId>) -> StorageResult<()> {
        self.db
            .put_cf(self.store(), b"vote", serde_json::to_vec(vote).unwrap().as_slice())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Write, AnyError::new(&e)),
            })
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<GlareNodeId>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"vote")
            .map_err(|e| StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Read, AnyError::new(&e)))?
            .and_then(|v| serde_json::from_slice(&v).ok())
        )
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<ConsensusSnapshot>> {
        Ok(self
            .db
            .get_cf(self.store(), b"current_snapshot")
            .map_err(|e| StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)))?
            .and_then(|v| serde_json::from_slice(&v).ok())
        )
    }

    fn set_current_snapshot_(&self, snapshot: ConsensusSnapshot) -> StorageResult<()> {
        self.db
            .put_cf(self.store(), b"current_snapshot", serde_json::to_vec(&snapshot).unwrap().as_slice())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(ErrorSubject::Snapshot(snapshot.meta.signature()), ErrorVerb::Write, AnyError::new(&e)),
            })?;

        Ok(())
    }
}

#[async_trait]
impl RaftStorage<GlareTypeConfig> for Arc<ConsensusStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<GlareNodeId>,
    ) -> Result<(), StorageError<GlareNodeId>> {
        self.set_vote_(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<GlareNodeId>>, StorageError<GlareNodeId>> {
        self.get_vote_()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<GlareTypeConfig>]
    ) -> StorageResult<()> {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);

            assert_eq!(bin_to_id(&id), entry.log_id.index);

            self.db
                .put_cf(
                    self.logs(),
                    id,
                    serde_json::to_vec(entry)
                        .map_err(|e| StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)))?,
                )
                .map_err(|e| StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)))?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<GlareNodeId>,
    ) -> StorageResult<()> {
        tracing::debug!("delete_logs_since: [{:?}, +oo)", log_id);

        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);

        self.db
            .delete_range_cf(self.logs(), &from, &to)
            .map_err(|e| StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)).into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<GlareNodeId>,
    ) -> StorageResult<()> {
        tracing::debug!("purge_logs_upto: [{:?}, +oo)", log_id);

        self.set_last_purged_(log_id)?;

        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);

        self.db
            .delete_range_cf(self.logs(), &from, &to)
            .map_err(|e| StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)).into())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GlareNodeId>>,
            EffectiveMembership<GlareNodeId, GlareNode>,
        ),
        StorageError<GlareNodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<GlareTypeConfig>],
    ) -> Result<Vec<GlareResponse>, StorageError<GlareNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.set_last_applied_log(entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => res.push(GlareResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    GlareRequest::Set { key, value } => {
                        sm.insert(key.clone(), value.clone())?;
                        res.push(GlareResponse {
                            value: Some(value.clone()),
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.set_last_membership(EffectiveMembership::new(Some(entry.log_id), mem.clone()))?;
                    res.push(GlareResponse { value: None })
                }
            };
        }

        self.db
            .flush_wal(true)
            .map_err(|e| StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)))?;

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Self::SnapshotData>, StorageError<GlareNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GlareNodeId, GlareNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<GlareTypeConfig>, StorageError<GlareNodeId>> {
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
            let updated_state_machine: SerializableConsensusStateMachine = serde_json::from_slice(&new_snapshot.data)
                .map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = ConsensusStateMachine::from_serializable(updated_state_machine, self.db.clone())?;
        }

        self.set_current_snapshot_(new_snapshot)?;
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GlareNodeId, GlareNode, Self::SnapshotData>>, StorageError<GlareNodeId>> {
        match ConsensusStore::get_current_snapshot_(self)? {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
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
impl RaftLogReader<GlareTypeConfig> for Arc<ConsensusStore> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<GlareTypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.logs(), rocksdb::IteratorMode::End)
            .next()
            .and_then(|r| {
                if let Ok((_, ent)) = r {
                    Some(serde_json::from_slice::<Entry<GlareTypeConfig>>(&ent).unwrap().log_id)
                } else {
                    None
                }
            });

        let last_purged_log_id = self.get_last_purged_()?;

        let last_log_id = match last {
            None => last_purged_log_id,
            x => x,
        };

        Ok(LogState {
            last_log_id,
            last_purged_log_id,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<GlareTypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        self.db
            .iterator_cf(self.logs(), rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward))
            .map(|r| {
                if let Ok((id, val)) = r {
                    let entry: StorageResult<Entry<_>> = serde_json::from_slice(&val).map_err(|e| StorageError::IO {
                        source: StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)),
                    });
                    let id = bin_to_id(&id);

                    assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                    (id, entry)
                } else {
                    todo!();
                }
            })
            .take_while(|(id, _)| range.contains(id))
            .map(|x| x.1)
            .collect()
    }
}

#[async_trait]
impl RaftSnapshotBuilder<GlareTypeConfig, Cursor<Vec<u8>>> for Arc<ConsensusStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GlareNodeId, GlareNode, Cursor<Vec<u8>>>, StorageError<GlareNodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // serialize the data of the satate machine
            let state_machine = SerializableConsensusStateMachine::from(&*self.state_machine.read().await);
            data = serde_json::to_vec(&state_machine)
                .map_err(|e| StorageIOError::new(ErrorSubject::StateMachine, ErrorVerb::Read, AnyError::new(&e)))?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
        }

        // TODO: make this atomic
        let snapshot_idx: u64 = self.get_snapshot_index_()? + 1;
        self.set_snapshot_index_(snapshot_idx)?;

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

        self.set_current_snapshot_(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl ConsensusStore {
    pub(crate) async fn new<P: AsRef<Path>>(db_path: P) -> Arc<ConsensusStore> {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let store = rocksdb::ColumnFamilyDescriptor::new("store", rocksdb::Options::default());
        let state_machine = rocksdb::ColumnFamilyDescriptor::new("state_machine", rocksdb::Options::default());
        let data = rocksdb::ColumnFamilyDescriptor::new("data", rocksdb::Options::default());
        let logs = rocksdb::ColumnFamilyDescriptor::new("logs", rocksdb::Options::default());

        let db = rocksdb::DB::open_cf_descriptors(&db_opts, db_path.as_ref(), vec![store, state_machine, data, logs]).unwrap();

        let db = Arc::new(db);
        let state_machine = RwLock::new(ConsensusStateMachine::new(db.clone()));
        Arc::new(ConsensusStore { db, state_machine })
    }
}
