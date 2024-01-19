use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{Stream, StreamExt};
use serde_json::json;
use telemetry::Tracker;
use uuid::Uuid;

use crate::metrics::AggregatedMetrics;

/// Result type used when we don't know the result of a query yet.
const UNKNOWN_RESULT_TYPE: &str = "unknown";

/// Pushes metrics to the telemetry tracker for the open session.
#[derive(Debug, Clone)]
pub struct SessionMetricsHandler {
    user_id: Uuid,
    database_id: Uuid,
    connection_id: Uuid,
    tracker: Arc<Tracker>,
}

impl SessionMetricsHandler {
    pub fn new(
        user_id: Uuid,
        database_id: Uuid,
        connection_id: Uuid,
        tracker: Arc<Tracker>,
    ) -> SessionMetricsHandler {
        SessionMetricsHandler {
            user_id,
            database_id,
            connection_id,
            tracker,
        }
    }

    /// Push a metrics directly into the metrics vector.
    ///
    /// This will also push the metric out to Segment.
    pub fn push_metric(&self, metric: QueryMetrics) {
        self.tracker.track(
            "Execution metric",
            self.user_id,
            json!({
                // Additional info.
                "database_id": self.database_id.hyphenated().encode_lower(&mut Uuid::encode_buffer()),
                "connection_id": self.connection_id.hyphenated().encode_lower(&mut Uuid::encode_buffer()),

                // Metric fields.
                "query_text": metric.query_text,
                "telemetry_tag": metric.result_type,
                "execution_status": metric.execution_status.as_str(),
                "error_message": metric.error_message,
                "elapsed_compute_ns": metric.elapsed_compute_ns,
                "output_rows": metric.output_rows,
                "bytes_read": metric.bytes_read,
                "bytes_written": metric.bytes_written,
            }),
        );
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
    pub result_type: &'static str,
    pub execution_status: ExecutionStatus,
    /// Error message if the query failed.
    pub error_message: Option<String>,
    /// Elapsed compute in nanoseconds for the query. Currently only set for
    /// SELECT queries.
    pub elapsed_compute_ns: Option<u64>,
    /// Number of output rows. Currently only set for SELECT queries.
    pub output_rows: Option<u64>,
    /// Number of bytes processed during the execution of query.
    pub bytes_read: Option<u64>,
    /// Number of bytes written during the execution of write operation.
    pub bytes_written: Option<u64>,
}

impl Default for QueryMetrics {
    fn default() -> Self {
        Self {
            query_text: "<empty>".to_string(),
            result_type: UNKNOWN_RESULT_TYPE,
            execution_status: ExecutionStatus::Unknown,
            error_message: None,
            elapsed_compute_ns: None,
            output_rows: None,
            bytes_read: None,
            bytes_written: None,
        }
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
    /// Session metrics handler.
    metrics_handler: SessionMetricsHandler,
}

impl BatchStreamWithMetricSender {
    pub fn new(
        stream: SendableRecordBatchStream,
        plan: Arc<dyn ExecutionPlan>,
        pending: QueryMetrics,
        metrics_handler: SessionMetricsHandler,
    ) -> Self {
        BatchStreamWithMetricSender {
            stream,
            plan,
            pending: Some(pending),
            metrics_handler,
        }
    }

    pub fn record_batch_metrics(&self, metrics: &mut QueryMetrics) {
        if let Some(exec_metrics) = self.plan.metrics() {
            metrics.output_rows = exec_metrics.output_rows().map(|v| v as u64);
        }

        let agg_metrics = AggregatedMetrics::new_from_plan(self.plan.as_ref());
        metrics.bytes_read = Some(agg_metrics.bytes_read);
        metrics.bytes_written = agg_metrics.bytes_written;
        metrics.elapsed_compute_ns = Some(agg_metrics.elapsed_compute_ns);
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
                    self.record_batch_metrics(&mut metrics);
                    self.metrics_handler.push_metric(metrics);
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
                    self.record_batch_metrics(&mut metrics);
                    self.metrics_handler.push_metric(metrics);
                }

                Poll::Ready(Some(Err(e)))
            }
            poll => poll,
        }
    }
}
