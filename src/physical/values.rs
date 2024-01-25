use crate::errors::{err, Result};
use crate::expr::PhysicalExpr;
use crate::hash::build_hashes;
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{Field, Schema};
use hashbrown::raw::RawTable;
use parking_lot::Mutex;
use smallvec::{smallvec, SmallVec};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct Values {
    schema: Arc<Schema>,
    partitions: Vec<Mutex<Option<RecordBatch>>>,
}

impl Source for Values {
    fn output_partitions(&self) -> usize {
        self.partitions.len()
    }

    fn poll_partition(
        &self,
        _cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut partition = self.partitions[partition].lock();
        match partition.take() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => Poll::Ready(None),
        }
    }
}
