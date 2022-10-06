use crate::errors::{internal, Result};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct DeltaCache {
    inserts: HashMap<u32, RwLock<Vec<RecordBatch>>>,
}

impl DeltaCache {
    fn insert(&self, part_id: u32, batch: RecordBatch) -> Result<()> {
        unimplemented!()
    }

    fn scan(&self, part_id: u32) -> Result<Vec<RecordBatch>> {
        unimplemented!()
    }
}
