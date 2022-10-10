use crate::errors::Result;
use crate::partition::Partition;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, Distribution, ExecutionPlan,
    Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::trace;

/// A physical plan node for returning delta record batches for a single
/// partition.
#[derive(Debug)]
pub struct PartitionExec {}

#[async_trait]
impl ExecutionPlan for PartitionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn output_partitioning(&self) -> Partitioning {
        unimplemented!()
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        unimplemented!()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

pub type StreamOpenFuture = BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch>>>>;

pub trait StreamOpener: Unpin {
    fn open(&self) -> Result<StreamOpenFuture>;
}

enum StreamState {
    Idle,
    Open {
        future: StreamOpenFuture,
    },
    Scan {
        stream: BoxStream<'static, Result<RecordBatch>>,
    },
    Error,
    Done,
}

pub struct PartitionStream<S> {
    /// Current stream state.
    state: StreamState,
    /// Opens a record batch stream.
    opener: S,
    schema: SchemaRef,
}

impl<S: StreamOpener> PartitionStream<S> {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Idle => match self.opener.open() {
                    Ok(fut) => {
                        self.state = StreamState::Open { future: fut };
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                },
                StreamState::Open { future } => match ready!(future.poll_unpin(cx)) {
                    Ok(stream) => {
                        self.state = StreamState::Scan { stream };
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                },
                StreamState::Scan { stream } => match ready!(stream.poll_next_unpin(cx)) {
                    Some(result) => {
                        return Poll::Ready(Some(result.map_err(|e| e.into())));
                    }
                    None => self.state = StreamState::Done,
                },
                StreamState::Error | StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl<S: StreamOpener> Stream for PartitionStream<S> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl<S: StreamOpener> RecordBatchStream for PartitionStream<S> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
