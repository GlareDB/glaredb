use lemur::{repr::{relation::RelationKey, df::Schema, expr::ScalarExpr}, execute::stream::source::{ReadTx, WriteTx, DataFrameStream}};
use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageIOError};
use std::{error::Error, fmt::Debug, sync::Arc};
use storageengine::rocks::RocksStore;

use crate::openraft_types::types::{EffectiveMembership, LogId, StorageError};

use super::StorageResult;

pub(super) mod serializable;

#[derive(Clone, Debug)]
pub struct ConsensusStateMachine {
    pub db: Arc<rocksdb::DB>,
    pub inner: RocksStore,
}

pub(super) trait RaftStateMachine {
    fn get_last_membership(&self) -> StorageResult<EffectiveMembership>;
    fn set_last_membership(&self, membership: EffectiveMembership) -> StorageResult<()>;
    fn get_last_applied_log(&self) -> StorageResult<Option<LogId>>;
    fn set_last_applied_log(&self, log_id: LogId) -> StorageResult<()>;
}

impl ConsensusStateMachine {
    pub fn new(db: Arc<rocksdb::DB>) -> Self {
        Self {
            db: db.clone(),
            inner: RocksStore::with_db(db),
        }
    }

    pub(super) fn insert(&self, key: String, value: String) -> StorageResult<()> {
        self.db
            .put_cf(
                self.db.cf_handle("data").unwrap(),
                key.as_bytes(),
                value.as_bytes(),
            )
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
            })
    }

    pub fn get(&self, key: &str) -> StorageResult<Option<String>> {
        let key = key.as_bytes();
        self.db
            .get_cf(self.db.cf_handle("data").unwrap(), key)
            .map(|value| {
                value.map(|value| String::from_utf8(value.to_vec()).expect("invalid data"))
            })
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
            })
    }

    pub fn begin(&self) -> StorageResult<u64> {
        Ok(self.inner.begin().get_id())
    }

    pub async fn get_schema(&self, table: &RelationKey) -> StorageResult<Option<Schema>> {
        let tx = self.inner.begin();
        
        let schema = tx.get_schema(table).await.map_err(|e| {
            StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::error(e.to_string()))
        })?;

        Ok(schema)
    }

    pub async fn scan(&self, table: &RelationKey, filter: Option<ScalarExpr>) -> StorageResult<Option<DataFrameStream>> {
        let tx = self.inner.begin();
        
        let stream = tx.scan(table, filter).await.map_err(|e| {
            StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::error(e.to_string()))
        })?;

        Ok(stream)
    }

    pub async fn allocate_table(&self, table: RelationKey, schema: Schema) -> StorageResult<()> {
        let tx = self.inner.begin();

        tx.allocate_table(table, schema).await.map_err(|e| {
            StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::error(e.to_string()))
        })?;

        Ok(())
    }
}

impl RaftStateMachine for ConsensusStateMachine {
    fn get_last_membership(&self) -> StorageResult<EffectiveMembership> {
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

    fn set_last_membership(&self, membership: EffectiveMembership) -> StorageResult<()> {
        self.db
            .put_cf(
                self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_membership".as_bytes(),
                serde_json::to_vec(&membership).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    fn get_last_applied_log(&self) -> StorageResult<Option<LogId>> {
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

    fn set_last_applied_log(&self, log_id: LogId) -> StorageResult<()> {
        self.db
            .put_cf(
                self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_applied_log".as_bytes(),
                serde_json::to_vec(&log_id).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }
}

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&e),
    )
    .into()
}

fn sm_w_err<E: Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}
