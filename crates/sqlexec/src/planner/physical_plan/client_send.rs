use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use protogen::gen::rpcsrv::common;
use tracing::debug;
use uuid::Uuid;

use crate::errors::Result;
use crate::remote::client::RemoteSessionClient;

/// Execution plan for sending batches to a remote node.
#[derive(Debug)]
pub struct ClientExchangeSendExec {
    pub database_id: Uuid,
    pub work_id: Uuid,
    pub client: RemoteSessionClient,
    pub input: Arc<dyn ExecutionPlan>,
}

impl ClientExchangeSendExec {
    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "send_count",
            DataType::UInt64,
            false,
        )]))
    }
}

impl ExecutionPlan for ClientExchangeSendExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Self::arrow_schema()
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Cannot change children for ClientExchangeSendExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        debug!(%partition, %self.work_id, "executing client exchange send exec");
        // Supporting multiple partitions in the future should be easy enough,
        // just make more streams.
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "ClientExchangeSendExec only supports 1 partition".to_string(),
            ));
        }

        let input = self.input.execute(0, context)?;
        let stream = ClientExchangeSendStream::new(self.client.database_id(), self.work_id, input);

        let fut = flush_stream(self.client.clone(), stream);
        let stream = futures::stream::once(fut);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Self::arrow_schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for ClientExchangeSendExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientExchangeInputSendExec: work_id={}", self.work_id)
    }
}

/// Results for client exchange send.
#[derive(Debug, Default)]
struct ClientExchangeSendResult {
    /// Numer of rows we sent.
    pub row_count: usize,

    /// Error if we encountered one.
    pub error: Option<DataFusionError>,
}

/// Stream for sending record batches to a server.
///
/// Every poll to the underlying record batch stream will encode the batch and
/// produce the request message with the correct fields set.
// TODO: There's some overlap with `ExecutionResponseBatchStream`, not sure if
// we want to try to unify.
struct ClientExchangeSendStream {
    /// Database this stream is for.
    database_id: Uuid,

    /// Unique identifier for this stream.
    work_id: Uuid,

    /// The underlying batch stream.
    stream: SendableRecordBatchStream,

    /// IPC encoding buffer.
    buf: Vec<u8>,

    /// Track number of rows written.
    row_count: usize,

    /// Results of the stream. Only contains accurate data _after_ the stream
    /// completes.
    ///
    /// Is this jank? Yes, since we're reading from a stream of results (the
    /// record batch stream), and producing a stream of ipc encoded data, we
    /// don't really have a decent way forwarding errors through the stream.
    result: Arc<Mutex<ClientExchangeSendResult>>,
}

impl ClientExchangeSendStream {
    fn new(database_id: Uuid, work_id: Uuid, stream: SendableRecordBatchStream) -> Self {
        ClientExchangeSendStream {
            database_id,
            work_id,
            stream,
            buf: Vec::new(),
            row_count: 0,
            result: Arc::new(Mutex::new(ClientExchangeSendResult::default())),
        }
    }

    /// Get a reference to the stream results.
    ///
    /// Should only be checked after the stream completes.
    fn result_ref(&self) -> Arc<Mutex<ClientExchangeSendResult>> {
        self.result.clone()
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<common::ExecutionResultBatch> {
        self.buf.clear();

        let schema = batch.schema();
        let mut writer = IpcFileWriter::try_new(&mut self.buf, &schema)?;
        writer.write(batch)?;
        writer.finish()?;

        let _ = writer.into_inner()?;

        Ok(common::ExecutionResultBatch {
            database_id: self.database_id.as_bytes().to_vec(),
            arrow_ipc: self.buf.clone(),
            work_id: self.work_id.as_bytes().to_vec(),
        })
    }
}

impl fmt::Debug for ClientExchangeSendStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientExchangeSendStream")
            .field("database_id", &self.database_id)
            .field("work_id", &self.work_id)
            .finish_non_exhaustive()
    }
}

impl Stream for ClientExchangeSendStream {
    type Item = common::ExecutionResultBatch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.row_count += batch.num_rows();
                let req = match self.write_batch(&batch) {
                    Ok(req) => req,
                    Err(e) => {
                        let mut result = self.result.lock();
                        result.error = Some(DataFusionError::Execution(format!(
                            "failed to encode batch: {e}"
                        )));
                        return Poll::Ready(None);
                    }
                };

                Poll::Ready(Some(req))
            }
            Poll::Ready(Some(Err(e))) => {
                let mut result = self.result.lock();
                result.error = Some(e);
                Poll::Ready(None)
            }
            Poll::Ready(None) => {
                // if row_count is 0 and we are here that means the stream is empty.
                // send an empty batch record in this case, so that we are able to extract
                // session_id and broadcast_id from the stream.
                if self.row_count == 0 {
                    self.row_count += 1;
                    let empty_batch = RecordBatch::new_empty(self.stream.schema());
                    let req = match self.write_batch(&empty_batch) {
                        Ok(req) => req,
                        Err(e) => {
                            let mut result = self.result.lock();
                            result.error = Some(DataFusionError::Execution(format!(
                                "failed to encode empty batch: {e}"
                            )));
                            return Poll::Ready(None);
                        }
                    };
                    Poll::Ready(Some(req))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Helper for flushing a stream to the remote client, only returning once all
/// batches have been flushed, or an error occurs.
///
/// On success, the resulting record batch will contain a single return row with
/// the count of rows that were sent.
///
/// Any errors encountered during flushing will be returned.
async fn flush_stream(
    mut client: RemoteSessionClient,
    stream: ClientExchangeSendStream,
) -> DataFusionResult<RecordBatch> {
    let result_ref = stream.result_ref();
    client.broadcast_exchange(stream).await.map_err(|e| {
        DataFusionError::Execution(format!("failed to stream broadcast exchange: {e}"))
    })?;

    // Get error/row count from results.
    let mut result = result_ref.lock();

    // We errored during the stream, we want to bubble that up.
    if let Some(e) = result.error.take() {
        return Err(e);
    }

    // Create record batch with row count calculated by the stream.
    let batch = RecordBatch::try_new(
        ClientExchangeSendExec::arrow_schema(),
        vec![Arc::new(UInt64Array::new(
            vec![result.row_count as u64].into(),
            None,
        ))],
    )?;

    Ok(batch)
}
