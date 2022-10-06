use crate::errors::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Maximum number of uncompacted batches before we compact.
const MAX_UNCOMPACTED_BATCHES: usize = 128;

/// Most recent modifications to a table.
///
/// Adding a modification to this delta does not guarantee durability in any
/// way.
pub struct TableDelta {
    approx_size: AtomicUsize,
    schema: SchemaRef,
    inner: Arc<RwLock<TableDeltaInner>>,
}

struct TableDeltaInner {
    inserts: TableInserts,
}

struct TableInserts {
    /// The most recent batches we've received.
    latest: Vec<RecordBatch>,
    /// All the previous record batches.
    rest: Vec<RecordBatch>,
}

impl TableInserts {
    /// Compact all record batches into a single record, returning the result.
    fn compact_all(&self, schema: &SchemaRef) -> Result<RecordBatch> {
        let rest = RecordBatch::concat(schema, &self.rest[..])?;
        let latest = RecordBatch::concat(schema, &self.latest[..])?;
        let all = RecordBatch::concat(schema, &[rest, latest])?;
        Ok(all)
    }

    fn clear(&mut self) {
        self.latest.clear();
        self.rest.clear();
    }
}

impl TableDelta {
    /// Insert a batch as a table delta.
    ///
    /// This batch must have the same schema as the table. Any casting should be
    /// done beforehand.
    pub fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        let mut inner = self.inner.write();
        inner.inserts.latest.push(batch);

        if inner.inserts.latest.len() > MAX_UNCOMPACTED_BATCHES {
            let batch = RecordBatch::concat(&self.schema, &inner.inserts.latest[..])?;

            let batch_size: usize = batch
                .columns()
                .iter()
                .map(|col| col.get_buffer_memory_size())
                .sum();
            self.approx_size.fetch_add(batch_size, Ordering::Relaxed);

            inner.inserts.rest.push(batch);
            inner.inserts.latest.clear();
        }

        Ok(())
    }

    /// Return the approximate size of this delta in bytes.
    pub fn approx_size_bytes(&self) -> usize {
        self.approx_size.load(Ordering::Relaxed)
    }
}
