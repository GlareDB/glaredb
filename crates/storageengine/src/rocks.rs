use crate::repr::{InternalValue, Key, PrimaryKey, PrimaryKeyIndices, TableId};
use anyhow::{anyhow, Result};
use lemur::repr::value::Row;
use parking_lot::{Mutex, RwLock};
use rocksdb::{Options, DB};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::debug;

const DB_FILENAME: &str = "rocks.db";

#[derive(Debug)]
pub struct StorageConfig {
    pub data_dir: String,
}

/// A storage implementation backed by RocksDB.
#[derive(Debug, Clone)]
pub struct RocksStore {
    inner: Arc<InnerDb>,
}

impl RocksStore {
    pub fn open(conf: StorageConfig) -> Result<RocksStore> {
        let path = Path::new(&conf.data_dir).join(DB_FILENAME);
        let db = DB::open_default(path)?;
        Ok(RocksStore {
            inner: Arc::new(InnerDb {
                conf,
                db,
                active_txs: RwLock::new(BTreeMap::new()),
            }),
        })
    }

    pub fn begin(&self) -> StorageTxRef {
        static ID_GEN: AtomicU64 = AtomicU64::new(0);
        let id = ID_GEN.fetch_add(1, Ordering::Relaxed);
        let tx = Arc::new(StorageTx {
            id,
            inner: self.inner.clone(),
        });

        {
            let mut active = self.inner.active_txs.write();
            active.insert(id, tx.clone());
        }
        tx
    }

    pub fn resume(&self, id: u64) -> Option<StorageTxRef> {
        let active = self.inner.active_txs.read();
        active.get(&id).cloned()
    }
}

#[derive(Debug)]
struct InnerDb {
    conf: StorageConfig,
    db: DB,
    active_txs: RwLock<BTreeMap<u64, StorageTxRef>>,
}

impl InnerDb {
    fn remove(&self, id: u64) {
        let mut active = self.active_txs.write();
        if let None = active.remove(&id) {
            // TODO: This may happen if there's multiple outstanding references
            // that commit/abort at the same time. Ideally we can add a barrier
            // of some sort ensuring that this doesn't happen.
            debug!(id, "attempted to remove transaction we don't know about");
        }
    }
}

pub type StorageTxRef = Arc<StorageTx>;

#[derive(Debug)]
pub struct StorageTx {
    id: u64,
    inner: Arc<InnerDb>,
}

impl StorageTx {
    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn commit(self) -> Result<()> {
        self.inner.remove(self.id);
        Ok(())
    }

    pub fn abort(self) -> Result<()> {
        self.inner.remove(self.id);
        Ok(())
    }

    pub fn insert(&self, table: TableId, idxs: PrimaryKeyIndices<'_>, row: Row) -> Result<()> {
        let mut pk = Vec::with_capacity(idxs.len());
        for idx in idxs.iter() {
            pk.push(
                row.values
                    .get(*idx)
                    .cloned()
                    .ok_or(anyhow!("missing value for pk idx: {}", idx))?,
            );
        }

        let key = Key::Primary(table, pk);
        let val = InternalValue::PrimaryRecord(row);

        self.inner.db.put(key.serialize()?, val.serialize()?)?;

        Ok(())
    }

    pub fn delete(&self, table: TableId, pk: PrimaryKey<'_>) -> Result<()> {
        let buf = Key::Primary(table, pk.to_vec()).serialize()?;
        self.inner
            .db
            .put(&buf, InternalValue::Tombstone.serialize()?)?;
        Ok(())
    }

    pub fn get(&self, table: TableId, pk: PrimaryKey<'_>) -> Result<Option<Row>> {
        let buf = Key::Primary(table, pk.to_vec()).serialize()?;
        match self.inner.db.get_pinned(&buf)? {
            Some(val) => {
                let internal = InternalValue::deserialize(val)?;
                match internal {
                    InternalValue::PrimaryRecord(row) => Ok(Some(row)),
                    InternalValue::Tombstone => Ok(None),
                    other => Err(anyhow!("unexpected internal value: {:?}", other)),
                }
            }
            None => Ok(None),
        }
    }
}
