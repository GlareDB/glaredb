use crate::expr::PhysicalExpr;
use crate::hash::build_hashes;
use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};
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

/// Aggregates with no grouping.
pub struct UngroupedAggregate {}

impl Explainable for UngroupedAggregate {
    fn explain_entry(_conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("UngroupedAggregate")
    }
}
