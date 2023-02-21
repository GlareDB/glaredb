use crate::engine::SessionInfo;
use crate::errors::Result;
use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DatafusionResult;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{Stream, StreamExt};
use metastore::builtins::GLARE_SESSION_QUERY_METRICS;
use serde_json::json;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, pin::Pin};
use telemetry::Tracker;
use tokio::sync::mpsc;
use tracing::error;
use uuid::Uuid;

/// Number of query metrics to hold in-memory. Once exceeded, the oldest metric
/// gets dropped.
const MAX_METRICS_HISTORY: usize = 100;

/// Holds some number of query metrics for a session.
///
/// TODO: It may be more efficient to just store these directly in a record
/// batch instead of recreating one every time this gets queried.
#[derive(Debug)]
pub struct SessionMetrics {
    info: Arc<SessionInfo>,
    tracker: Arc<Tracker>,

    completed_rx: mpsc::Receiver<QueryMetrics>,
    completed_tx: mpsc::Sender<QueryMetrics>,

    metrics: VecDeque<QueryMetrics>,
}

impl SessionMetrics {
    pub fn new(info: Arc<SessionInfo>, tracker: Arc<Tracker>) -> SessionMetrics {
        let (tx, rx) = mpsc::channel(1);
        SessionMetrics {
            info,
            tracker,
            completed_rx: rx,
            completed_tx: tx,
            metrics: VecDeque::new(),
        }
    }

    /// Get an mpsc sender for use during async query executions (any query that
    /// streams back record batches).
    pub fn get_sender(&self) -> mpsc::Sender<QueryMetrics> {
        self.completed_tx.clone()
    }

    /// Flush any completed metrics into the underlying metrics vector.
    ///
    /// This should be called prior to execution of any statements.
    pub fn flush_completed(&mut self) {
        if let Ok(m) = self.completed_rx.try_recv() {
            self.push_metric(m);
        }
    }

    /// Push a metrics directly into the metrics vector.
    ///
    /// This will also push the metric out to Segment.
    pub fn push_metric(&mut self, metric: QueryMetrics) {
        // TODO: Segment
        self.metrics.push_front(metric);
        if self.metrics.len() > MAX_METRICS_HISTORY {
            self.metrics.pop_back();
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &QueryMetrics> {
        self.metrics.iter()
    }

    pub fn num_metrics(&self) -> usize {
        self.metrics.len()
    }
}

#[derive(Debug)]
pub enum ExecutionStatus {
    Success,
    Fail,
    Unknown,
}

impl ExecutionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionStatus::Success => "success",
            ExecutionStatus::Fail => "fail",
            ExecutionStatus::Unknown => "unknown",
        }
    }
}

/// A set of metrics for a single query.
///
/// Every query should have a set of metrics associated with it. Some fields may
/// not be relevant for a particular query type, and should be Null/None in such
/// cases.
#[derive(Debug)]
pub struct QueryMetrics {
    pub query_text: String,
    pub execution_status: ExecutionStatus,
    pub error_message: Option<String>,
    pub elapsed_compute: u64,
    pub output_rows: Option<u32>,
}

impl QueryMetrics {
    pub fn to_json_value(&self) -> serde_json::Value {
        json!({
            "query_text": self.query_text,
            "execution_status": self.execution_status.as_str(),
            "error_message": self.error_message.clone().unwrap_or_default(),
            "elapsed_compute": self.elapsed_compute,
            "output_rows": self.output_rows.unwrap_or(0),
        })
    }
}

/// A wrapper around a batch stream that will send a completed query metric onto
/// a channel.
pub struct BatchStreamWithMetricSender {
    /// Underlying stream being wrapped.
    stream: SendableRecordBatchStream,
    /// Reference to the plan to get complete query metrics from.
    plan: Arc<dyn ExecutionPlan>,
    /// The pending set of query metrics. Wrapped in an Option to allow taking
    /// inner.
    pending: Option<QueryMetrics>,
    /// Channel to send complete metrics on.
    sender: mpsc::Sender<QueryMetrics>,
}

impl BatchStreamWithMetricSender {
    pub fn new(
        stream: SendableRecordBatchStream,
        plan: Arc<dyn ExecutionPlan>,
        pending: QueryMetrics,
        sender: mpsc::Sender<QueryMetrics>,
    ) -> Self {
        BatchStreamWithMetricSender {
            stream,
            plan,
            pending: Some(pending),
            sender,
        }
    }
}

impl RecordBatchStream for BatchStreamWithMetricSender {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl Stream for BatchStreamWithMetricSender {
    type Item = DatafusionResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(None) => {
                // Stream finished, add finishing touches to metrics and send on
                // channel.

                if let Some(mut metrics) = self.pending.take() {
                    metrics.execution_status = ExecutionStatus::Success;

                    if let Some(exec_metrics) = self.plan.metrics() {
                        metrics.elapsed_compute =
                            exec_metrics.elapsed_compute().unwrap_or(0) as u64;
                        metrics.output_rows = exec_metrics.output_rows().map(|v| v as u32);
                    }

                    if let Err(e) = self.sender.try_send(metrics) {
                        error!(%e,"failed to send completed metrics on channel");
                    }
                }

                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                // Stream returned error. Go ahead and mark this query as
                // failed. Note that the streaming batch logic in pgsrv returns
                // after the first error, the stream won't be polled to
                // completion, so we go ahead and send the metric now.

                if let Some(mut metrics) = self.pending.take() {
                    metrics.execution_status = ExecutionStatus::Fail;
                    metrics.error_message = Some(e.to_string());

                    // The query may have failed, but having these execution
                    // stats may be useful anyways.
                    if let Some(exec_metrics) = self.plan.metrics() {
                        metrics.elapsed_compute =
                            exec_metrics.elapsed_compute().unwrap_or(0) as u64;
                        metrics.output_rows = exec_metrics.output_rows().map(|v| v as u32);
                    }

                    if let Err(e) = self.sender.try_send(metrics) {
                        error!(%e,"failed to send completed metrics on channel");
                    }
                }

                Poll::Ready(Some(Err(e)))
            }
            poll => poll,
        }
    }
}
