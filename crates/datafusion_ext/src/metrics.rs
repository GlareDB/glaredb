use datafusion::{
    arrow::datatypes::SchemaRef,
    arrow::record_batch::RecordBatch,
    error::Result,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, Gauge, MetricBuilder},
        ExecutionPlan, RecordBatchStream,
    },
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

const BYTES_PROCESSED_GAUGE_NAME: &str = "bytes_processed";

/// Standard metrics we should be collecting for all data sources during
/// queries.
#[derive(Debug, Clone)]
pub struct DataSourceMetrics {
    /// Track bytes processed by source plans.
    pub bytes_processed: Gauge,

    /// Baseline metrics like output rows and elapsed time.
    pub baseline: BaselineMetrics,
}

impl DataSourceMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let bytes_processed =
            MetricBuilder::new(metrics).gauge(BYTES_PROCESSED_GAUGE_NAME, partition);
        let baseline = BaselineMetrics::new(metrics, partition);

        Self {
            bytes_processed,
            baseline,
        }
    }

    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch>>>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if let Poll::Ready(maybe_batch) = &poll {
            match maybe_batch {
                Some(Ok(batch)) => {
                    self.bytes_processed.add(batch.get_array_memory_size());
                    self.baseline.record_output(batch.num_rows());
                }
                Some(Err(_)) => self.baseline.done(),
                None => self.baseline.done(),
            }
        }
        poll
    }
}

/// Thin wrapper around a record batch stream that automatically records metrics
/// about batches that are sent through the stream.
pub struct MetricsStreamAdapter<S> {
    pub stream: S,
    pub metrics: DataSourceMetrics,
}

impl<S> MetricsStreamAdapter<S> {
    /// Create a new stream with a new set of data source metrics for the given
    /// partition.
    pub fn new(stream: S, partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            stream,
            metrics: DataSourceMetrics::new(partition, metrics),
        }
    }
}

impl<S: RecordBatchStream + Unpin> Stream for MetricsStreamAdapter<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.stream.poll_next_unpin(cx);
        self.metrics.record_poll(poll)
    }
}

impl<S: RecordBatchStream + Unpin> RecordBatchStream for MetricsStreamAdapter<S> {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

#[derive(Debug, Clone)]
pub struct AggregatedMetrics {
    /// Total time taken for a plan to execute.
    pub elapsed_compute_ns: u64,
    /// Total output rows for the plan.
    pub output_rows: u64,
    /// Total bytes processed.
    pub bytes_processed: u64,
}

impl AggregatedMetrics {
    /// Computes aggregated metrics from a plan.
    ///
    /// The plan should have already been executed to completion, otherwise
    /// partial or incorrect results will be reported.
    pub fn new_from_plan(plan: &dyn ExecutionPlan) -> Self {
        let mut agg = AggregatedMetrics {
            elapsed_compute_ns: 0,
            output_rows: 0,
            bytes_processed: 0,
        };

        agg.aggregate_recurse(plan);

        agg
    }

    fn aggregate_recurse(&mut self, plan: &dyn ExecutionPlan) {
        // TODO: What to do about output rows?

        let metrics = match plan.metrics() {
            Some(metrics) => metrics,
            None => return,
        };
        self.elapsed_compute_ns += metrics.elapsed_compute().unwrap_or_default() as u64;
        self.bytes_processed += metrics
            .sum_by_name(BYTES_PROCESSED_GAUGE_NAME)
            .map(|m| m.as_usize() as u64)
            .unwrap_or_default();

        for child in plan.children() {
            self.aggregate_recurse(child.as_ref());
        }
    }
}
