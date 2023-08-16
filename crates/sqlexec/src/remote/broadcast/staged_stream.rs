use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::exchange_exec::{ClientExchangeRecvExec, ClientExchangeRecvStream};

/// State of the pending stream.
enum PendingStream {
    /// The stream arrived first. Store both the receiver and
    WaitingForTableCreate,
}

/// Holds streams that are awaiting scans.
pub struct StagedTableStreams {
    /// Streams keyed by broadcast id.
    streams: HashMap<Uuid, oneshot::Sender<ClientExchangeRecvStream>>,
}

impl StagedTableStreams {
    pub fn put_stream(&mut self, id: Uuid, stream: ClientExchangeRecvStream) {
        // If we have a "table" waiting, go ahead and trigger the oneshot with
        // the stream. This will trigger scanning of the "table".
        if let Some(channel) = self.streams.remove(&id) {
            channel.send(stream);
            return;
        }

        // Otherwise stage it.
        // let
        // self.streams.insert(id, on)
    }
}
