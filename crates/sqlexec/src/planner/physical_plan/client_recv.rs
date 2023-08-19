use crate::remote::exchange_stream::ClientExchangeRecvStream;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{FutureExt, Stream, StreamExt};
use protogen::ProtoConvError;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uuid::Uuid;

use crate::remote::staged_stream::{ResolveClientStreamFut, StagedClientStreams};

/// The actual execution plan for reading batches from the client.
///
/// Expects the task context to have `StagedClientStreams` available.
#[derive(Debug)]
pub struct ClientExchangeRecvExec {
    broadcast_id: Uuid,
    schema: Arc<Schema>,
}

impl ClientExchangeRecvExec {
    pub fn new(broadcast_id: Uuid, schema: Arc<Schema>) -> Self {
        ClientExchangeRecvExec {
            schema,
            broadcast_id,
        }
    }
}

impl ExecutionPlan for ClientExchangeRecvExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot change children for ClientExchangeInputReadExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "ClientExchangeInputReadExec only supports 1 partition".to_string(),
            ));
        }

        let streams = context
            .session_config()
            .get_extension::<StagedClientStreams>()
            .ok_or_else(|| {
                DataFusionError::Execution("Missing stages streams extension".to_string())
            })?;

        let stream_fut = streams.resolve_pending_stream(self.broadcast_id);
        let stream = ClientExchangeStateStream::Pending {
            fut: stream_fut,
            schema: self.schema(),
        };

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for ClientExchangeRecvExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientExchangeInputReadExec")
    }
}

impl TryFrom<protogen::sqlexec::physical_plan::ClientExchangeRecvExec> for ClientExchangeRecvExec {
    type Error = ProtoConvError;
    fn try_from(
        value: protogen::sqlexec::physical_plan::ClientExchangeRecvExec,
    ) -> Result<Self, Self::Error> {
        let id = Uuid::from_slice(&value.broadcast_id)?;
        let schema = value.schema.ok_or(ProtoConvError::RequiredField(
            "schema is required".to_string(),
        ))?;
        // TODO: Upstream `TryFrom` impl that doesn't need a reference.
        let schema: Schema = (&schema).try_into()?;

        Ok(Self {
            broadcast_id: id,
            schema: Arc::new(schema),
        })
    }
}

// TODO: We'll likely need to generalize this for use in some of our other
// physical plans.
enum ClientExchangeStateStream {
    /// Resolving the stream.
    Pending {
        fut: ResolveClientStreamFut,
        schema: Arc<Schema>,
    },
    /// We have the stream.
    Stream(ClientExchangeRecvStream),
}

impl Stream for ClientExchangeStateStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Pending { fut, .. } => {
                match fut.poll_unpin(cx) {
                    Poll::Ready(Ok(mut stream)) => {
                        // Get first poll of the stream.
                        let poll = stream.poll_next_unpin(cx);
                        // Store the stream for the next iteration.
                        *self = Self::Stream(stream);
                        // Return first poll result.
                        poll
                    }
                    Poll::Ready(Err(e)) => Poll::Ready(Some(Err(DataFusionError::Execution(
                        format!("failed resolving pending stream: {e}"),
                    )))),
                    Poll::Pending => Poll::Pending,
                }
            }
            Self::Stream(stream) => stream.poll_next_unpin(cx),
        }
    }
}

impl RecordBatchStream for ClientExchangeStateStream {
    fn schema(&self) -> Arc<Schema> {
        match self {
            Self::Pending { schema, .. } => schema.clone(),
            Self::Stream(s) => s.schema(),
        }
    }
}
