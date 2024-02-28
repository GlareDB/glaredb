use std::collections::VecDeque;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use protogen::gen::rpcsrv::common;
use tonic::Streaming;
use uuid::Uuid;

use crate::errors::{ExecError, Result};

/// A stream for reading record batches from a client.
///
/// The first message is used to get the session id. It's assumed that the
/// stream contains batches all with the same schema.
#[derive(Debug)]
pub struct ExecutionBatchStream {
    /// Database this stream is for.
    ///
    /// This is used to get a valid session on the remote side.
    database_id: Uuid,

    /// Unique id for this input.
    work_id: Uuid,

    /// The grpc stream.
    ///
    /// It's assumed that every batch read has the same schema as the first
    /// batch read.
    stream: Streaming<common::ExecutionResultBatch>,

    /// A single request may include multiple batches, include those here.
    buf: VecDeque<DataFusionResult<RecordBatch>>,

    /// Batches schema.
    schema: Arc<Schema>,
}

impl ExecutionBatchStream {
    /// Try to create a new stream from a grpc stream.
    ///
    /// A oneshot receiver is returned allowing the caller to await until the
    /// stream is complete. This is useful in the grpc handler to await stream
    /// completion before returning a response.
    pub async fn try_new(
        mut input: Streaming<common::ExecutionResultBatch>,
    ) -> Result<ExecutionBatchStream> {
        let req = input
            .next()
            .await
            .ok_or_else(|| ExecError::RemoteSession("stream missing first IPC batch".to_string()))?
            .map_err(ExecError::from)?;

        let work_id = Uuid::from_slice(&req.work_id)
            .map_err(|e| ExecError::RemoteSession(format!("get work id: {e}")))?;

        let database_id = Uuid::from_slice(&req.database_id)
            .map_err(|e| ExecError::RemoteSession(format!("get database id: {e}")))?;

        // Get first set of batches (primarily for the schema)
        let batches: VecDeque<_> = Self::read_arrow_ipc(req.arrow_ipc)?.collect();
        let schema = match batches.front() {
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

        Ok(ExecutionBatchStream {
            database_id,
            work_id,
            stream: input,
            buf: batches,
            schema,
        })
    }

    pub fn database_id(&self) -> Uuid {
        self.database_id
    }

    pub fn work_id(&self) -> Uuid {
        self.work_id
    }

    fn read_arrow_ipc(
        buf: Vec<u8>,
    ) -> DataFusionResult<impl Iterator<Item = DataFusionResult<RecordBatch>>> {
        let cursor = Cursor::new(buf);
        let reader = IpcFileReader::try_new(cursor, None)?;
        Ok(reader.into_iter().map(|result| match result {
            Ok(batch) => Ok(batch),
            Err(e) => Err(DataFusionError::ArrowError(e, None)),
        }))
    }
}

// TODO: StreamReader instead of FileReader.
impl Stream for ExecutionBatchStream {
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

impl RecordBatchStream for ExecutionBatchStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
