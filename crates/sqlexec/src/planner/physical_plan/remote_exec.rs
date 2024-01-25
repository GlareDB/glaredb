use datafusion::arrow::datatypes::Schema as ArrowSchema;
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

use crate::remote::client::RemoteSessionClient;

/// Execute a physical plan on a remote service.
#[derive(Debug, Clone)]
pub struct RemoteExecutionExec {
    client: RemoteSessionClient,
    /// The plan to send to the remote service.
    plan: Arc<dyn ExecutionPlan>,
    /// The query text to send for collecting metrics.
    query_text: String,
}

impl RemoteExecutionExec {
    pub fn new(
        client: RemoteSessionClient,
        plan: Arc<dyn ExecutionPlan>,
        query_text: String,
    ) -> Self {
        RemoteExecutionExec {
            client,
            plan,
            query_text,
        }
    }
}

impl ExecutionPlan for RemoteExecutionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.plan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.plan.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RemoteExecutionExec {
            client: self.client.clone(),
            plan: children[0].clone(),
            query_text: self.query_text.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // TODO: Behavior is unknown when executing with more than one
        // partition.
        if partition != 0 {
            return Err(DataFusionError::Execution(format!("RemoteExecutionExec only supports 1 partition, got request for partition {partition}")));
        }

        let stream = stream::once(execute_remote(
            self.client.clone(),
            self.plan.clone(),
            self.query_text.clone(),
        ))
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        self.plan.statistics()
    }
}

impl DisplayAs for RemoteExecutionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteExecutionExec: deployment={}",
            self.client.get_deployment_name()
        )
    }
}

/// Execute the encoded logical plan on the remote service.
async fn execute_remote(
    mut client: RemoteSessionClient,
    plan: Arc<dyn ExecutionPlan>,
    query_text: String,
) -> DataFusionResult<ExecutionResponseBatchStream> {
    let stream = client
        .physical_plan_execute(plan, query_text)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to execute physical plan on remote service: {e}"
            ))
        })?;

    Ok(ExecutionResponseBatchStream {
        stream,
        buf: VecDeque::new(),
    })
}

/// Converts a response stream from the service into a record batch stream.
// TODO: StreamReader instead of FileReader.
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
                Err(e) => {
                    let msg = e.message();
                    Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                        "Remote node error: {msg}"
                    )))))
                }
            },
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
