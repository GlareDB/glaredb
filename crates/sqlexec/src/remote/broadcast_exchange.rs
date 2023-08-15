use crate::errors::{ExecError, Result};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use protogen::gen::rpcsrv::service;
use std::any::Any;
use std::fmt;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tonic::Streaming;
use uuid::Uuid;

/// A stream for reading record batches from a client.
///
/// The first message is used to get the session id. It's assumed that the
/// stream contains batches all with the same schema.
#[derive(Debug)]
pub struct ClientExchangeInputStream {
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

impl ClientExchangeInputStream {
    /// Try to create a new stream from a grpc stream.
    pub async fn try_new(
        mut input: Streaming<service::BroadcastExchangeRequest>,
    ) -> Result<ClientExchangeInputStream> {
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

        Ok(ClientExchangeInputStream {
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
impl Stream for ClientExchangeInputStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check buffer.
        if let Some(batch) = self.buf.pop_front() {
            return Poll::Ready(Some(batch));
        }

        // Pull from stream
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(req)) => match req {
                Ok(req) => {
                    let batches = match Self::read_arrow_ipc(req.arrow_ipc) {
                        Ok(iter) => iter,
                        Err(e) => return Poll::Ready(Some(Err(e))),
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

impl RecordBatchStream for ClientExchangeInputStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// The actual execution plan for reading batches from the client.
#[derive(Debug)]
pub struct ClientExchangeInputReadExec {
    schema: Arc<Schema>,
    stream: Mutex<Option<ClientExchangeInputStream>>,
}

impl ClientExchangeInputReadExec {
    pub fn new(stream: ClientExchangeInputStream) -> Self {
        let schema = stream.schema();
        ClientExchangeInputReadExec {
            schema,
            stream: Mutex::new(Some(stream)),
        }
    }
}

impl ExecutionPlan for ClientExchangeInputReadExec {
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
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "ClientExchangeInputReadExec only supports 1 partition".to_string(),
            ));
        }

        let mut stream = self.stream.lock();
        let stream = match stream.take() {
            Some(stream) => stream,
            None => return Err(DataFusionError::Execution(
                format!("ClientExchangeInputReadExec::execute called more than once for partition {partition}"),
            )),
        };

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for ClientExchangeInputReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientExchangeInputReadExec")
    }
}
