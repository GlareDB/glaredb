use crate::errors::{ExecError, Result};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use protogen::gen::rpcsrv::service;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tonic::Streaming;
use uuid::Uuid;

/// A set of input record batch streams being exchanged.
pub struct StagedStreams {
    streams: Mutex<HashMap<Uuid, SendableRecordBatchStream>>,
}

impl StagedStreams {
    /// Try to put a stream in the staged streams.
    ///
    /// This will read the first record batch before placing the stream in the
    /// map.
    pub async fn try_put_stream(
        &self,
        mut input: Streaming<service::BroadcastExchangeRequest>,
    ) -> Result<()> {
        let req = input
            .next()
            .await
            .ok_or_else(|| ExecError::RemoteSession("stream missing first IPC batch".to_string()))?
            .map_err(ExecError::from)?;

        unimplemented!()
    }
}

/// Input stream from a client.
struct BroadcastExchangeRequestStream {
    /// The grpc stream.
    stream: Streaming<service::BroadcastExchangeRequest>,

    /// A single request may include multiple batches, include those here.
    buf: VecDeque<DataFusionResult<RecordBatch>>,

    /// Batches schema.
    schema: Arc<Schema>,
}

impl Stream for BroadcastExchangeRequestStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }
}

impl RecordBatchStream for BroadcastExchangeRequestStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
