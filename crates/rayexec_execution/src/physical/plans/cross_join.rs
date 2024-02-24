use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use std::task::{Context, Poll};

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct PhysicalCrossJoin {}

impl Explainable for PhysicalCrossJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CrossJoin")
    }
}
