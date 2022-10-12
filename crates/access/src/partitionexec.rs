use crate::errors::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, Distribution, ExecutionPlan,
    Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{
    future::{BoxFuture, FutureExt},
    ready,
    stream::{BoxStream, StreamExt},
    Stream,
};
use object_store::{path::Path as ObjectPath, ObjectStore};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct PartitionExec {
    schema: SchemaRef,
}

impl ExecutionPlan for PartitionExec {
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
        Err(DataFusionError::Execution(
            "cannot replace children for PartitionExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        unimplemented!()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PartitionExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Debug)]
pub struct PartitionMeta {
    pub location: ObjectPath,
}

pub type PartitionOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, ArrowResult<RecordBatch>>>>;

pub trait PartitionStreamOpener: Unpin {
    fn open(&self, meta: &PartitionMeta) -> Result<PartitionOpenFuture>;
}

struct ParquetPartitionOpener {
    store: Arc<dyn ObjectStore>,
}

impl PartitionStreamOpener for ParquetPartitionOpener {
    fn open(&self, meta: &PartitionMeta) -> Result<PartitionOpenFuture> {
        let read_opts = ArrowReaderOptions::new().with_page_index(true);

        Ok(Box::pin(async move {
            //
            unimplemented!()
        }))
    }
}

enum StreamState {
    /// File is not currently being read.
    Idle,
    /// File is being opened.
    Open { fut: PartitionOpenFuture },
    /// The file completed opening with the given stream.
    Scan {
        stream: BoxStream<'static, ArrowResult<RecordBatch>>,
    },
    /// Done scanning.
    Done,
    /// Encountered and error.
    Error,
}

pub struct PartitionStream<P> {
    schema: SchemaRef,
    meta: PartitionMeta,
    opener: P,
    state: StreamState,
}

impl<P: PartitionStreamOpener> PartitionStream<P> {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Idle => match self.opener.open(&self.meta) {
                    Ok(fut) => {
                        self.state = StreamState::Open { fut };
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                },
                StreamState::Open { fut } => match ready!(fut.poll_unpin(cx)) {
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
                        return Poll::Ready(Some(result));
                    }
                    None => {
                        self.state = StreamState::Done;
                    }
                },
                StreamState::Done | StreamState::Error => return Poll::Ready(None),
            }
        }
    }
}

impl<P: PartitionStreamOpener> Stream for PartitionStream<P> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl<P: PartitionStreamOpener> RecordBatchStream for PartitionStream<P> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
