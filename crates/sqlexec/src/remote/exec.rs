use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use protogen::gen::rpcsrv::service::RecordBatchResponse;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::Streaming;
use uuid::Uuid;

use super::client::RemoteSessionClient;

/// Execute a logical plan on a remote service.
#[derive(Debug, Clone)]
pub struct RemoteExecutionPlan {
    client: RemoteSessionClient,
    /// The logical plan to execute remotely.
    exec_id: Uuid,
    /// Schema for the execution plan.
    schema: SchemaRef,
}

impl RemoteExecutionPlan {
    pub fn new(client: RemoteSessionClient, exec_id: Uuid, schema: SchemaRef) -> Self {
        RemoteExecutionPlan {
            client,
            exec_id,
            schema,
        }
    }

    pub fn id(&self) -> Uuid {
        self.exec_id
    }
}

impl ExecutionPlan for RemoteExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
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
            "replacing children for RemoteLogicalExec not supported".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "RemoteLocalExec only supports 1 partition".to_string(),
            ));
        }

        let stream = stream::once(execute_remote(self.client.clone(), self.exec_id)).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for RemoteExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RemoteExecutionPlan")
    }
}

/// Execute the encoded logical plan on the remote service.
async fn execute_remote(
    mut client: RemoteSessionClient,
    exec_id: Uuid,
) -> DataFusionResult<ExecutionResponseBatchStream> {
    let stream = client.physical_plan_execute(exec_id).await.map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to execute logical plan on remote service: {e}"
        ))
    })?;

    Ok(ExecutionResponseBatchStream {
        stream,
        buf: VecDeque::new(),
    })
}

/// Converts a response stream from the service into a record batch stream.
// TODO: StreamReader
struct ExecutionResponseBatchStream {
    /// Stream we're reading from.
    stream: Streaming<RecordBatchResponse>,

    /// Buffer in case the ipc message contains more than one batch.
    buf: VecDeque<DataFusionResult<RecordBatch>>,
}

impl Stream for ExecutionResponseBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check buffer first.
        if let Some(batch) = self.buf.pop_front() {
            return Poll::Ready(Some(batch));
        }

        // Pull from stream.
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(resp)) => match resp {
                Ok(resp) => {
                    let cursor = Cursor::new(resp.arrow_ipc);
                    let reader = match IpcFileReader::try_new(cursor, None) {
                        Ok(reader) => reader,
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                                "failed to create arrow ipc reader: {e}"
                            )))))
                        }
                    };

                    // Extend out buffer with batches from the ipc reader.
                    self.buf
                        .extend(reader.into_iter().map(|result| match result {
                            Ok(batch) => Ok(batch),
                            Err(e) => Err(DataFusionError::ArrowError(e)),
                        }));

                    // See if we got anything.
                    match self.buf.pop_front() {
                        Some(result) => Poll::Ready(Some(result)),
                        None => Poll::Ready(None),
                    }
                }
                Err(e) => Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                    "failed to pull next batch from stream: {e}"
                ))))),
            },
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
