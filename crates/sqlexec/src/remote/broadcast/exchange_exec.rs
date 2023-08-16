//! Execution plan implementations for exchanging of record batches between a
//! client and server.
use crate::errors::{ExecError, Result};
use crate::remote::client::RemoteSessionClient;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::{
    reader::FileReader as IpcFileReader, writer::FileWriter as IpcFileWriter,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{FutureExt, Stream, StreamExt};
use parking_lot::Mutex;
use protogen::gen::rpcsrv::service;
use std::any::Any;
use std::fmt;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{collections::VecDeque, sync::Arc};
use tonic::Streaming;
use uuid::Uuid;

use super::staged_stream::{ResolveClientStreamFut, StagedClientStreams};

/// A stream for reading record batches from a client.
///
/// The first message is used to get the session id. It's assumed that the
/// stream contains batches all with the same schema.
#[derive(Debug)]
pub struct ClientExchangeRecvStream {
    /// Session this stream is for.
    session_id: Uuid,

    /// Unique id for this input.
    broadcast_id: Uuid,

    /// The grpc stream.
    ///
    /// It's assumed that every batch read has the same schema as the first
    /// batch read.
    stream: Streaming<service::BroadcastExchangeRequest>,

    /// A single request may include multiple batches, include those here.
    buf: VecDeque<DataFusionResult<RecordBatch>>,

    /// Batches schema.
    schema: Arc<Schema>,
}

impl ClientExchangeRecvStream {
    /// Try to create a new stream from a grpc stream.
    ///
    /// A oneshot receiver is returned allowing the caller to await until the
    /// stream is complete. This is useful in the grpc handler to await stream
    /// completion before returning a response.
    pub async fn try_new(
        mut input: Streaming<service::BroadcastExchangeRequest>,
    ) -> Result<ClientExchangeRecvStream> {
        let req = input
            .next()
            .await
            .ok_or_else(|| ExecError::RemoteSession("stream missing first IPC batch".to_string()))?
            .map_err(ExecError::from)?;

        let broadcast_id = Uuid::from_slice(&req.broadcast_input_id)
            .map_err(|e| ExecError::RemoteSession(format!("get session id: {e}")))?;

        let session_id = Uuid::from_slice(&req.session_id)
            .map_err(|e| ExecError::RemoteSession(format!("get session id: {e}")))?;

        // Get first set of batches (primarily for the schema)
        let batches: VecDeque<_> = Self::read_arrow_ipc(req.arrow_ipc)?.collect();
        let schema = match batches.get(0) {
            Some(Ok(batch)) => batch.schema(),
            Some(Err(e)) => {
                return Err(ExecError::RemoteSession(format!(
                    "Reading first batch error: {e}"
                )))
            }
            None => {
                return Err(ExecError::RemoteSession(
                    "Missing first batch on input stream".to_string(),
                ))
            }
        };

        Ok(ClientExchangeRecvStream {
            session_id,
            broadcast_id,
            stream: input,
            buf: batches,
            schema,
        })
    }

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub fn broadcast_id(&self) -> Uuid {
        self.broadcast_id
    }

    fn read_arrow_ipc(
        buf: Vec<u8>,
    ) -> DataFusionResult<impl Iterator<Item = DataFusionResult<RecordBatch>>> {
        let cursor = Cursor::new(buf);
        let reader = IpcFileReader::try_new(cursor, None)?;
        Ok(reader.into_iter().map(|result| match result {
            Ok(batch) => Ok(batch),
            Err(e) => Err(DataFusionError::ArrowError(e)),
        }))
    }
}

// TODO: StreamReader instead of FileReader.
impl Stream for ClientExchangeRecvStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check buffer.
        if let Some(batch) = self.buf.pop_front() {
            return Poll::Ready(Some(batch));
        }

        // Pull from stream.
        //
        // Trigger oneshot on error or fi the stream completes.
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(req)) => match req {
                Ok(req) => {
                    let batches = match Self::read_arrow_ipc(req.arrow_ipc) {
                        Ok(iter) => iter,
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    // Extend out buffer with batches from the ipc reader.
                    self.buf.extend(batches);

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

impl RecordBatchStream for ClientExchangeRecvStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// The actual execution plan for reading batches from the client.
#[derive(Debug)]
pub struct ClientExchangeRecvExec {
    schema: Arc<Schema>,
    broadcast_id: Uuid,
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
    /// Remote ID of the session this stream is for.
    session_id: Uuid,

    /// Unique identifier for this stream.
    broadcast_id: Uuid,

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
    fn new(session_id: Uuid, broadcast_id: Uuid, stream: SendableRecordBatchStream) -> Self {
        ClientExchangeSendStream {
            session_id,
            broadcast_id,
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

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<service::BroadcastExchangeRequest> {
        self.buf.clear();

        let schema = batch.schema();
        let mut writer = IpcFileWriter::try_new(&mut self.buf, &schema)?;
        writer.write(batch)?;
        writer.finish()?;

        let _ = writer.into_inner()?;

        Ok(service::BroadcastExchangeRequest {
            arrow_ipc: self.buf.clone(),
            session_id: self.session_id.as_bytes().to_vec(),
            broadcast_input_id: self.broadcast_id.as_bytes().to_vec(),
        })
    }
}

impl fmt::Debug for ClientExchangeSendStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientExchangeSendStream")
            .field("session_id", &self.session_id)
            .field("broadcast_id", &self.broadcast_id)
            .finish_non_exhaustive()
    }
}

impl Stream for ClientExchangeSendStream {
    type Item = service::BroadcastExchangeRequest;

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
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Execution plan for sending batches.
#[derive(Debug)]
pub struct ClientExchangeInputSendExec {
    broadcast_id: Uuid,
    client: RemoteSessionClient,
    input: Arc<dyn ExecutionPlan>,
}

impl ClientExchangeInputSendExec {
    pub fn new(
        broadcast_id: Uuid,
        client: RemoteSessionClient,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            broadcast_id,
            client,
            input,
        }
    }

    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "send_count",
            DataType::UInt64,
            false,
        )]))
    }
}

impl ExecutionPlan for ClientExchangeInputSendExec {
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
        // Supporting multiple partitions in the future should be easy enough,
        // just make more streams.
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "ClientExchangeInputSendExec only supports 1 partition".to_string(),
            ));
        }

        let input = self.input.execute(0, context)?;
        let stream =
            ClientExchangeSendStream::new(self.client.session_id(), self.broadcast_id, input);

        let fut = flush_stream(self.client.clone(), stream);
        let stream = futures::stream::once(fut);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Self::arrow_schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for ClientExchangeInputSendExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientExchangeInputSendExec")
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
        ClientExchangeInputSendExec::arrow_schema(),
        vec![Arc::new(UInt64Array::new(
            vec![result.row_count as u64].into(),
            None,
        ))],
    )?;

    Ok(batch)
}
