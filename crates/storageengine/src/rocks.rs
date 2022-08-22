use lemur::repr::df::{DataFrame, Schema};
use lemur::repr::expr::ScalarExpr;
use lemur::repr::relation::{PrimaryKey, PrimaryKeyIndices, RelationKey};
use lemur::repr::value::Row;
use parking_lot::RwLock;
use rocksdb::{Direction, IteratorMode, DB};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::debug;

use crate::errors::{Result, StorageError};
use crate::repr::{InternalValue, Key};

const DB_FILENAME: &str = "rocks.db";

#[derive(Debug)]
pub struct StorageConfig {
    pub data_dir: String,
}

/// A simple storage implementation backed by RocksDB.
#[derive(Debug, Clone)]
pub struct RocksStore {
    inner: Arc<InnerDb>,
}

impl RocksStore {
    /// Open a store backed by a RocksDB instance with the given config.
    pub fn open(conf: StorageConfig) -> Result<RocksStore> {
        debug!("opening rocks store with conf: {:?}", conf);
        let path = Path::new(&conf.data_dir).join(DB_FILENAME);
        let db = DB::open_default(path)?;
        Ok(RocksStore {
            inner: Arc::new(InnerDb {
                db,
                active_txs: RwLock::new(BTreeMap::new()),
            }),
        })
    }

    /// Begin a transaction.
    // TODO: There's currently no transactional semantics.
    pub fn begin(&self) -> StorageTx {
        static ID_GEN: AtomicU64 = AtomicU64::new(0);
        let id = ID_GEN.fetch_add(1, Ordering::Relaxed);
        let tx = StorageTx {
            id,
            inner: self.inner.clone(),
        };

        {
            let mut active = self.inner.active_txs.write();
            active.insert(id, tx.clone());
        }
        tx
    }

    /// Resume an active transaction.
    pub fn resume(&self, id: u64) -> Option<StorageTx> {
        let active = self.inner.active_txs.read();
        active.get(&id).cloned()
    }
}

#[derive(Debug)]
struct InnerDb {
    db: DB,
    active_txs: RwLock<BTreeMap<u64, StorageTx>>,
}

impl InnerDb {
    fn remove(&self, id: u64) {
        let mut active = self.active_txs.write();
        if active.remove(&id).is_none() {
            // TODO: This may happen if there's multiple outstanding references
            // that commit/abort at the same time. Ideally we can add a barrier
            // of some sort ensuring that this doesn't happen.
            debug!(id, "attempted to remove transaction we don't know about");
        }
    }
}

#[derive(Debug, Clone)]
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

    pub fn store_schema(&self, table: RelationKey, schema: Schema) -> Result<()> {
        let key = Key::Schema(table);
        let val = InternalValue::Schema(schema);
        self.inner.db.put(key.serialize()?, val.serialize()?)?;
        Ok(())
    }

    pub fn read_schema(&self, table: RelationKey) -> Result<Option<Schema>> {
        let buf = Key::Schema(table.clone()).serialize()?;
        match self.inner.db.get_pinned(&buf)? {
            Some(val) => {
                let internal = InternalValue::deserialize(val)?;
                match internal {
                    InternalValue::Schema(schema) => Ok(Some(schema)),
                    other => Err(StorageError::UnexpectedInternalValue(other)),
                }
            }
            None => Ok(None),
        }
    }

    fn must_read_schema(&self, table: &RelationKey) -> Result<Schema> {
        match self.read_schema(table.clone())? {
            Some(schema) => Ok(schema),
            None => Err(StorageError::MissingSchemaForRelation(table.clone())),
        }
    }

    pub fn insert(&self, table: RelationKey, idxs: PrimaryKeyIndices<'_>, row: Row) -> Result<()> {
        let _ = self.must_read_schema(&table)?;

        let mut pk = Vec::with_capacity(idxs.len());
        for idx in idxs.iter() {
            pk.push(
                row.values
                    .get(*idx)
                    .cloned()
                    .ok_or(StorageError::MissingPkPart { idx: *idx })?,
            );
        }

        let key = Key::Primary(table, pk);
        let val = InternalValue::PrimaryRecord(row);

        self.inner.db.put(key.serialize()?, val.serialize()?)?;

        Ok(())
    }

    pub fn delete(&self, table: RelationKey, pk: PrimaryKey<'_>) -> Result<()> {
        let _ = self.must_read_schema(&table)?;

        let buf = Key::Primary(table, pk.to_vec()).serialize()?;
        self.inner
            .db
            .put(&buf, InternalValue::Tombstone.serialize()?)?;
        Ok(())
    }

    pub fn get(&self, table: RelationKey, pk: PrimaryKey<'_>) -> Result<Option<Row>> {
        let _ = self.must_read_schema(&table);

        let buf = Key::Primary(table, pk.to_vec()).serialize()?;
        match self.inner.db.get_pinned(&buf)? {
            Some(val) => {
                let internal = InternalValue::deserialize(val)?;
                match internal {
                    InternalValue::PrimaryRecord(row) => Ok(Some(row)),
                    InternalValue::Tombstone => Ok(None),
                    other => Err(StorageError::UnexpectedInternalValue(other)),
                }
            }
            None => Ok(None),
        }
    }

    pub fn scan(
        &self,
        table: RelationKey,
        begin: PrimaryKey<'_>,
        limit: usize,
        filter: Option<ScalarExpr>,
    ) -> Result<DataFrame> {
        let schema = self.must_read_schema(&table)?;

        let begin = Key::Primary(table.clone(), begin.to_vec()).serialize()?;
        let iter = self
            .inner
            .db
            .iterator(IteratorMode::From(&begin, Direction::Forward));

        let mut stacked_df = DataFrame::with_schema_and_capacity(&schema, limit)?;
        let mut rows_cap = limit;
        let mut rows = Vec::with_capacity(rows_cap);

        for item in iter {
            let (key, val) = item?;
            let key = Key::deserialize(&key)?;
            match key {
                Key::Primary(scanned, _) if scanned == table => {
                    let val = InternalValue::deserialize(&val)?;
                    if let InternalValue::PrimaryRecord(row) = val {
                        rows.push(row);
                        if rows.len() == rows_cap {
                            // We've reached the limit, create a new data frame
                            // from the rows we've collected.
                            let chunk_rows =
                                std::mem::replace(&mut rows, Vec::with_capacity(rows_cap));
                            let mut chunk = DataFrame::from_rows(chunk_rows)?;
                            if let Some(ref filter) = filter {
                                chunk = chunk.filter_expr(filter)?
                            }
                            // Filtering might've trimmed down the data frame,
                            // next iteration should try scan what's left to
                            // fill.
                            rows_cap -= chunk.num_rows();
                            stacked_df = stacked_df.clone().vstack(chunk)?;
                            // No more scanning needed.
                            if rows_cap == 0 {
                                break;
                            }
                        }
                    }
                }
                _ => break, // No longer scanning the same table.
            }
        }

        // Might not have processed rows yet, create a chunk and stack onto the
        // dataframe.
        let mut chunk = DataFrame::from_rows(std::mem::take(&mut rows))?;
        if let Some(ref filter) = filter {
            chunk = chunk.filter_expr(filter)?
        }
        stacked_df = stacked_df.clone().vstack(chunk)?;

        Ok(stacked_df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lemur::repr::expr::BinaryOperation;
    use lemur::repr::value::{Value, ValueType};
    use tempdir::TempDir;

    #[test]
    fn simple_scan() {
        logutil::init_test();

        let temp = TempDir::new("simple_scan").unwrap();
        let conf = StorageConfig {
            data_dir: temp.path().to_str().unwrap().to_string(),
        };
        let db = RocksStore::open(conf).unwrap();

        let table = "test_table".to_string();
        let tx = db.begin();

        let schema = vec![ValueType::Int32].into();
        tx.store_schema(table.clone(), schema).unwrap();

        let rows = vec![
            vec![Value::Int32(Some(4))],
            vec![Value::Int32(Some(6))],
            vec![Value::Int32(Some(7))],
            vec![Value::Int32(Some(8))],
        ]
        .into_iter()
        .map(Row::from);

        for row in rows {
            tx.insert(table.clone(), &[0], row).unwrap();
        }

        // Scan everything.
        let df = tx
            .scan(table.clone(), &[Value::Int32(Some(0))], 10, None)
            .unwrap();
        let expected = DataFrame::from_rows(
            vec![
                vec![Value::Int32(Some(4))],
                vec![Value::Int32(Some(6))],
                vec![Value::Int32(Some(7))],
                vec![Value::Int32(Some(8))],
            ]
            .into_iter()
            .map(Row::from),
        )
        .unwrap();
        assert_eq!(expected, df);

        // Scan from middle.
        let df = tx
            .scan(table.clone(), &[Value::Int32(Some(7))], 10, None)
            .unwrap();
        let expected = DataFrame::from_rows(
            vec![vec![Value::Int32(Some(7))], vec![Value::Int32(Some(8))]]
                .into_iter()
                .map(Row::from),
        )
        .unwrap();
        assert_eq!(expected, df);

        // Scan after end.
        let df = tx
            .scan(table.clone(), &[Value::Int32(Some(32))], 10, None)
            .unwrap();
        assert_eq!(0, df.num_rows());

        // Scan with filter.
        let filter = Some(ScalarExpr::Binary {
            op: BinaryOperation::Gt,
            left: ScalarExpr::Column(0).boxed(),
            right: ScalarExpr::Constant(Value::Int32(Some(5))).boxed(),
        });
        let df = tx
            .scan(table.clone(), &[Value::Int32(Some(0))], 10, filter)
            .unwrap();
        let expected = DataFrame::from_rows(
            vec![
                vec![Value::Int32(Some(6))],
                vec![Value::Int32(Some(7))],
                vec![Value::Int32(Some(8))],
            ]
            .into_iter()
            .map(Row::from),
        )
        .unwrap();
        assert_eq!(expected, df);

        // Scan with limit.
        let df = tx
            .scan(table.clone(), &[Value::Int32(Some(0))], 2, None)
            .unwrap();
        let expected = DataFrame::from_rows(
            vec![vec![Value::Int32(Some(4))], vec![Value::Int32(Some(6))]]
                .into_iter()
                .map(Row::from),
        )
        .unwrap();
        assert_eq!(expected, df);
    }
}
