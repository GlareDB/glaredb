use crate::errors::Result;
use crate::parquet::{ParquetOpener, ParquetStreamOpenFuture};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{
    future::FutureExt,
    ready,
    stream::{BoxStream, StreamExt},
    Stream,
};
use object_store::{ObjectMeta, ObjectStore};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Scan a partition that this node is responsible for.
#[derive(Debug)]
pub struct LocalPartitionExec {
    store: Arc<dyn ObjectStore>,
    /// Schema of the stream after projection.
    projected_schema: SchemaRef,
    /// Metadata about the file in object storage.
    meta: ObjectMeta,
    projection: Option<Vec<usize>>,
}

impl LocalPartitionExec {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        meta: ObjectMeta,
        input_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<LocalPartitionExec> {
        let projected_schema = match &projection {
            Some(projection) => Arc::new(input_schema.project(&projection)?),
            None => input_schema,
        };

        Ok(LocalPartitionExec {
            store,
            projected_schema,
            meta,
            projection,
        })
    }
}

impl ExecutionPlan for LocalPartitionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
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
            "cannot replace children for LocalPartitionExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let opener = ParquetOpener {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: None,
            projection: self.projection.clone(),
        };

        let stream = PartitionStream {
            schema: self.projected_schema.clone(),
            opener,
            state: StreamState::Idle,
        };

        Ok(Box::pin(stream))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LocalPartitionExec: projection={:?}", self.projection)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

enum StreamState {
    /// File is not currently being read.
    Idle,
    /// File is being opened.
    Open { fut: ParquetStreamOpenFuture },
    /// The file completed opening with the given stream.
    Scan {
        stream: BoxStream<'static, ArrowResult<RecordBatch>>,
    },
    /// Done scanning.
    Done,
    /// Encountered an error.
    Error,
}

pub struct PartitionStream {
    schema: SchemaRef,
    opener: ParquetOpener,
    state: StreamState,
}

impl PartitionStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Idle => match self.opener.open() {
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

impl Stream for PartitionStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl RecordBatchStream for PartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
