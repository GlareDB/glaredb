use crate::deltacache::DeltaCache;
use crate::errors::Result;
use crate::keys::PartitionKey;
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
    future::{BoxFuture, FutureExt},
    ready,
    stream::StreamExt,
    Stream,
};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Provides an execution stream that modifies some child stream with data
/// provided in the delta cache.
// TODO: Provide optional limit as well.
#[derive(Debug)]
pub struct DeltaMergeExec {
    partition: PartitionKey,
    /// The projected schema.
    schema: SchemaRef,
    deltas: Arc<DeltaCache>,
    projection: Option<Vec<usize>>,
    child: Arc<dyn ExecutionPlan>,
}

impl DeltaMergeExec {
    pub fn new(
        partition: PartitionKey,
        schema: SchemaRef,
        cache: Arc<DeltaCache>,
        projection: Option<Vec<usize>>,
        child: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema = match &projection {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };
        Ok(DeltaMergeExec {
            partition,
            schema,
            deltas: cache,
            projection,
            child,
        })
    }
}

impl ExecutionPlan for DeltaMergeExec {
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
        // NOTE: No children are returned here to prevent datafusion's optimizer
        // from trying to replace children.
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for DeltaMergeExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let stream = self.child.execute(partition, context)?;
        Ok(Box::pin(DeltaMergeStream {
            schema: self.schema.clone(),
            partition: self.partition.clone(),
            opener: self.deltas.clone(),
            stream,
            projection: self.projection.clone(),
            state: StreamState::Idle,
        }))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DeltaMergeExec: part={}, projection={:?}",
            self.partition, self.projection
        )
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

pub trait BatchModifierOpener<M: BatchModifier>: Unpin {
    /// Opens a `BatchModifier`.
    ///
    /// This will be useful if/when we need to read deltas from disk.
    fn open_modifier(
        &self,
        partition: &PartitionKey,
        schema: &SchemaRef,
    ) -> Result<BoxFuture<'static, M>>;
}

/// Describes how we modify a stream.
pub trait BatchModifier: Unpin {
    /// Given a record batch, make an necessary modifications to it.
    ///
    /// Record batches will not have projections applied. Projections will be
    /// applied on the returned record batch.
    fn modify(&self, batch: RecordBatch) -> Result<RecordBatch>;

    /// Return a stream for any remaining batches we need to send.
    ///
    /// Useful for returning data that has not yet been flushed to the
    /// underlying partition store.
    fn stream_rest(&self) -> SendableRecordBatchStream;
}

enum StreamState<M> {
    Idle,
    /// Open the modifier that will be modifying the stream.
    Open {
        fut: BoxFuture<'static, M>,
    },
    /// Read from the stream, modifying it as we go along.
    ReadStream {
        modifier: M,
    },
    /// Return the remainder from the delta.
    RemainderStream {
        stream: SendableRecordBatchStream,
    },
    /// Done scanning.
    Done,
    /// We encountered an error.
    Error,
}

pub struct DeltaMergeStream<M, O> {
    /// The projected schema.
    schema: SchemaRef,
    /// Partition that we're working on.
    partition: PartitionKey,
    /// The batch modifier opener.
    opener: Arc<O>,
    /// The child execution stream.
    stream: SendableRecordBatchStream,
    /// An optional projection on batches returned from the child and delta
    /// streams.
    projection: Option<Vec<usize>>,
    /// The current state of the future.
    state: StreamState<M>,
}

impl<M: BatchModifier, O: BatchModifierOpener<M>> DeltaMergeStream<M, O> {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Idle => match self.opener.open_modifier(&self.partition, &self.schema)
                {
                    Ok(fut) => self.state = StreamState::Open { fut },
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                },
                StreamState::Open { fut } => {
                    let modifier = ready!(fut.poll_unpin(cx));
                    self.state = StreamState::ReadStream { modifier };
                }
                StreamState::ReadStream { modifier } => {
                    match ready!(self.stream.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            // Get out modified batch, converting to an arrow
                            // error as necessary.
                            let mut result = modifier.modify(batch).map_err(|e| e.into());
                            // Apply optional projection.
                            if let Some(projection) = &self.projection {
                                result = result.and_then(|batch| batch.project(projection));
                            }
                            return Poll::Ready(Some(result));
                        }
                        Some(Err(e)) => {
                            self.state = StreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // Child stream finished. Get whatever is left in
                            // the delta.
                            self.state = StreamState::RemainderStream {
                                stream: modifier.stream_rest(),
                            };
                        }
                    }
                }
                StreamState::RemainderStream { stream } => match ready!(stream.poll_next_unpin(cx))
                {
                    Some(Ok(batch)) => match &self.projection {
                        Some(projection) => {
                            let result = batch.project(projection);
                            return Poll::Ready(Some(result));
                        }
                        None => return Poll::Ready(Some(Ok(batch))),
                    },
                    Some(Err(e)) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e)));
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

impl<M: BatchModifier, O: BatchModifierOpener<M>> Stream for DeltaMergeStream<M, O> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl<M: BatchModifier, O: BatchModifierOpener<M>> RecordBatchStream for DeltaMergeStream<M, O> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
