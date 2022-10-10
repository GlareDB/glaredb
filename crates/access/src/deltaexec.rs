use crate::deltacache::DeltaCache;
use crate::errors::AccessError;
use crate::keys::PartitionKey;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, Distribution, ExecutionPlan,
    Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::Stream;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A physical plan node for returning delta record batches for a single
/// partition.
#[derive(Debug)]
pub struct DeltaExec {
    cache: Arc<DeltaCache>,
    schema: SchemaRef,
    part: PartitionKey,
}

impl DeltaExec {
    pub fn new(part: PartitionKey, cache: Arc<DeltaCache>, schema: SchemaRef) -> DeltaExec {
        DeltaExec {
            cache,
            schema,
            part,
        }
    }
}

#[async_trait]
impl ExecutionPlan for DeltaExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn maintains_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "delta exec has no children"
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let batches = self
            .cache
            .clone_batches_for_part(&self.part)
            .map_err(AccessError::into_df)?;
        Ok(Box::pin(DeltaStream::new(self.schema.clone(), batches)))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaExec: partition={}", self.part)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

pub struct DeltaStream {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    idx: usize,
}

impl DeltaStream {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> DeltaStream {
        DeltaStream {
            schema,
            batches,
            idx: 0,
        }
    }
}

impl Stream for DeltaStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.idx >= self.batches.len() {
            return Poll::Ready(None);
        }
        let batch = self.batches[self.idx].clone();
        self.idx += 1;
        Poll::Ready(Some(Ok(batch)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.batches.len(), Some(self.batches.len()))
    }
}

impl RecordBatchStream for DeltaStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
