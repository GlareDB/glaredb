use datafusion::{
    arrow::datatypes::SchemaRef,
    arrow::{datatypes::Schema, record_batch::RecordBatch},
    error::Result,
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::{Stream, StreamExt};
use std::task::{Context, Poll};
use std::{any::Any, pin::Pin};
use std::{fmt, marker::PhantomData};
use std::{fmt::Debug, sync::Arc};

const BYTES_READ_GAUGE_NAME: &str = "bytes_read";
const BYTES_WRITTEN_GAUGE_NAME: &str = "bytes_written";

#[derive(Debug, Default)]
pub struct DataSourceMetricsOpts {
    pub track_reads: bool,
    pub track_writes: bool,
}

impl DataSourceMetricsOpts {
    pub const fn read_only() -> Self {
        Self {
            track_reads: true,
            track_writes: false,
        }
    }

    pub const fn write_only() -> Self {
        Self {
            track_reads: false,
            track_writes: true,
        }
    }
}

/// Standard metrics we should be collecting for all data sources during
/// queries.
#[derive(Debug, Clone)]
struct DataSourceMetrics {
    /// Track bytes read by source plans.
    bytes_read: Option<Gauge>,

    /// Track bytes written by the plan.
    bytes_written: Option<Gauge>,

    /// Baseline metrics like output rows and elapsed time.
    baseline: BaselineMetrics,
}

impl DataSourceMetrics {
    fn new(
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
        opts: DataSourceMetricsOpts,
    ) -> Self {
        let baseline = BaselineMetrics::new(metrics, partition);

        let bytes_read = if opts.track_reads {
            Some(MetricBuilder::new(metrics).gauge(BYTES_READ_GAUGE_NAME, partition))
        } else {
            None
        };

        let bytes_written = if opts.track_writes {
            Some(MetricBuilder::new(metrics).gauge(BYTES_WRITTEN_GAUGE_NAME, partition))
        } else {
            None
        };

        Self {
            bytes_read,
            bytes_written,
            baseline,
        }
    }

    /// Track metrics based on the poll result from an async stream.
    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch>>>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if let Poll::Ready(maybe_batch) = &poll {
            match maybe_batch {
                Some(Ok(batch)) => {
                    self.baseline.record_output(batch.num_rows());

                    let batch_size = batch.get_array_memory_size();

                    if let Some(bytes_read) = self.bytes_read.as_ref() {
                        bytes_read.add(batch_size);
                    }

                    if let Some(bytes_written) = self.bytes_written.as_ref() {
                        bytes_written.add(batch_size);
                    }
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
///
/// Note this should only be used when "ingesting" data during execution (data
/// sources or reading from tables) to avoid double counting bytes read.
pub struct DataSourceMetricsStreamAdapter<S> {
    stream: S,
    metrics: DataSourceMetrics,
}

impl<S> DataSourceMetricsStreamAdapter<S> {
    /// Create a new stream with a new set of data source metrics for the given
    /// partition.
    pub fn new(stream: S, partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            stream,
            metrics: DataSourceMetrics::new(partition, metrics, DataSourceMetricsOpts::read_only()),
        }
    }
}

impl<S: RecordBatchStream + Unpin> Stream for DataSourceMetricsStreamAdapter<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.stream.poll_next_unpin(cx);
        self.metrics.record_poll(poll)
    }
}

impl<S: RecordBatchStream + Unpin> RecordBatchStream for DataSourceMetricsStreamAdapter<S> {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

pub trait DataSourceMetricsOptsType: 'static + Debug + Clone + Sync + Send {
    const OPTS: DataSourceMetricsOpts;
    const DISPLAY_NAME_PREFIX: &'static str;
}

#[derive(Debug, Clone)]
pub struct ReadOnlyDataSourceMetricsOptsType;
impl DataSourceMetricsOptsType for ReadOnlyDataSourceMetricsOptsType {
    const OPTS: DataSourceMetricsOpts = DataSourceMetricsOpts::read_only();
    const DISPLAY_NAME_PREFIX: &'static str = "ReadOnly";
}

#[derive(Debug, Clone)]
pub struct WriteOnlyDataSourceMetricsOptsType;
impl DataSourceMetricsOptsType for WriteOnlyDataSourceMetricsOptsType {
    const OPTS: DataSourceMetricsOpts = DataSourceMetricsOpts::write_only();
    const DISPLAY_NAME_PREFIX: &'static str = "WriteOnly";
}

/// Wrapper around and execution plan that returns a
/// `BoxedDataSourceMetricsStreamAdapter` for additional metrics collection.
///
/// This should _generally_ only be used for execution plans that we're not
/// able to modify directly to record metrics (e.g. Delta). Otherwise, this
/// should be skipped and metrics collection should be added to the execution
/// plan directly.
#[derive(Debug, Clone)]
pub struct DataSourceMetricsExecAdapter<T: DataSourceMetricsOptsType> {
    child: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,

    _phantom: PhantomData<T>,
}

pub type ReadOnlyDataSourceMetricsExecAdapter =
    DataSourceMetricsExecAdapter<ReadOnlyDataSourceMetricsOptsType>;

pub type WriteOnlyDataSourceMetricsExecAdapter =
    DataSourceMetricsExecAdapter<WriteOnlyDataSourceMetricsOptsType>;

impl<T: DataSourceMetricsOptsType> DataSourceMetricsExecAdapter<T> {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            child: plan,
            metrics: ExecutionPlanMetricsSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<T: DataSourceMetricsOptsType> ExecutionPlan for DataSourceMetricsExecAdapter<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.child.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.child.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.child.execute(partition, context)?;
        Ok(Box::pin(BoxedStreamAdapater::new(
            stream,
            partition,
            &self.metrics,
            T::OPTS,
        )))
    }

    fn statistics(&self) -> Statistics {
        self.child.statistics()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl<T: DataSourceMetricsOptsType> DisplayAs for DataSourceMetricsExecAdapter<T> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}DataSourceMetricsExecAdapter", T::DISPLAY_NAME_PREFIX)
    }
}

struct BoxedStreamAdapater {
    stream: SendableRecordBatchStream,
    metrics: DataSourceMetrics,
}

impl BoxedStreamAdapater {
    fn new(
        stream: SendableRecordBatchStream,
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
        opts: DataSourceMetricsOpts,
    ) -> Self {
        Self {
            stream,
            metrics: DataSourceMetrics::new(partition, metrics, opts),
        }
    }
}

impl Stream for BoxedStreamAdapater {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.stream.poll_next_unpin(cx);
        self.metrics.record_poll(poll)
    }
}

impl RecordBatchStream for BoxedStreamAdapater {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

#[derive(Debug, Clone)]
pub struct AggregatedMetrics {
    /// Total time taken for a plan to execute.
    pub elapsed_compute_ns: u64,
    /// Total bytes read.
    pub bytes_read: u64,
    /// Total bytes written.
    pub bytes_written: Option<u64>,
}

impl AggregatedMetrics {
    /// Computes aggregated metrics from a plan.
    ///
    /// The plan should have already been executed to completion, otherwise
    /// partial or incorrect results will be reported.
    pub fn new_from_plan(plan: &dyn ExecutionPlan) -> Self {
        let mut agg = AggregatedMetrics {
            elapsed_compute_ns: 0,
            bytes_read: 0,
            bytes_written: None,
        };
        agg.aggregate_recurse(plan);
        agg
    }

    fn aggregate_recurse(&mut self, plan: &dyn ExecutionPlan) {
        if let Some(metrics) = plan.metrics() {
            self.elapsed_compute_ns += metrics.elapsed_compute().unwrap_or_default() as u64;
            self.bytes_read += metrics
                .sum_by_name(BYTES_READ_GAUGE_NAME)
                .map(|m| m.as_usize() as u64)
                .unwrap_or_default();

            if self.bytes_written.is_none() {
                // Only count bytes written if they were not counted before.
                // Writes can only happen at a higher level. If we've already
                // counted bytes written, any metrics in child plans are just
                // repetitions and we are counting the same thing again!
                self.bytes_written = metrics
                    .sum_by_name(BYTES_WRITTEN_GAUGE_NAME)
                    .map(|m| m.as_usize() as u64);
            }
        }

        for child in plan.children() {
            self.aggregate_recurse(child.as_ref());
        }
    }
}
