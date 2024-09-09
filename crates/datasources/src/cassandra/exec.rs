use datafusion::arrow::array::ArrayBuilder;
use datafusion::arrow::record_batch::RecordBatchOptions;

use super::builder::CqlValueArrayBuilder;
use super::{
    fmt,
    stream,
    Any,
    Arc,
    ArrowSchemaRef,
    Context,
    DataFusionError,
    DataSourceMetricsStreamAdapter,
    DatafusionResult,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    ExecutionPlanMetricsSet,
    MetricsSet,
    Partitioning,
    PhysicalSortExpr,
    Pin,
    Poll,
    RecordBatch,
    RecordBatchStream,
    Result,
    Row,
    SendableRecordBatchStream,
    Session,
    Statistics,
    Stream,
    StreamExt,
    TaskContext,
};

pub(super) struct CassandraExec {
    schema: ArrowSchemaRef,
    session: Arc<Session>,
    query: String,
    metrics: ExecutionPlanMetricsSet,
}

impl CassandraExec {
    pub(super) fn new(
        schema: ArrowSchemaRef,
        query: String,
        session: Arc<Session>,
    ) -> CassandraExec {
        CassandraExec {
            schema,
            session,
            query,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for CassandraExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
    fn output_partitioning(&self) -> Partitioning {
        // TODO: does scylla driver support partitioning?
        Partitioning::UnknownPartitioning(1)
    }
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for ScyllaExec".to_string(),
            ))
        }
    }
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "only single partition supported".to_string(),
            ));
        }

        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            CassandraRowStream::new(
                self.session.clone(),
                self.schema.clone(),
                self.query.clone(),
            ),
            partition,
            &self.metrics,
        )))
    }

    fn statistics(&self) -> DatafusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for CassandraExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ScyllaExec")
    }
}

impl fmt::Debug for CassandraExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaExec")
            .field("schema", &self.schema)
            .field("query", &self.query)
            .finish_non_exhaustive()
    }
}

pub(super) struct CassandraRowStream {
    schema: ArrowSchemaRef,
    inner: Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>,
}
impl Stream for CassandraRowStream {
    type Item = Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for CassandraRowStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

impl CassandraRowStream {
    pub fn new(session: Arc<Session>, schema: ArrowSchemaRef, query: String) -> Self {
        let schema_clone = schema.clone();
        let stream = stream! {
            match session.query_iter(query, &[]).await {
                Ok(res) => {
            let mut inner = res.chunks(1024);
                    while let Some(item) = inner.next().await {
            match item.into_iter().collect::<Result<Vec<_>, _>>() {
                Ok(rows) => yield rows_to_record_batch(Some(rows), schema_clone.clone()),
                Err(e) => {
                yield Err(DataFusionError::Execution(e.to_string()));
                return;
                }
            }
                    }
                },
                Err(e) => {
                    yield Err(DataFusionError::Execution(e.to_string()));
                    return;
                }
            };
        };

        Self {
            schema,
            inner: Box::pin(stream),
        }
    }
}
fn rows_to_record_batch(
    rows: Option<Vec<Row>>,
    schema: ArrowSchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    match rows {
        None => Ok(RecordBatch::new_empty(schema)),
        Some(rows) if schema.fields().is_empty() => {
            let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
            RecordBatch::try_new_with_options(schema, vec![], &options)
                .map_err(DataFusionError::from)
        }
        Some(rows) => {
            let mut builders = schema
                .fields()
                .iter()
                .map(|f| CqlValueArrayBuilder::new(f.data_type()))
                .collect::<Vec<_>>();

            for row in rows {
                for (i, value) in row.columns.into_iter().enumerate() {
                    builders[i].append_option(value);
                }
            }
            let columns = builders
                .into_iter()
                .map(|mut b| b.finish())
                .collect::<Vec<_>>();
            RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
        }
    }
}
