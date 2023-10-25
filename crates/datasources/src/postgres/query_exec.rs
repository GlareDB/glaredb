use std::{
    any::Any,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{datatypes::Schema as ArrowSchema, record_batch::RecordBatch},
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use futures::{future::BoxFuture, ready, FutureExt, Stream};

use crate::common::util::{create_count_record_batch, COUNT_SCHEMA};

use super::PostgresAccessState;

#[derive(Debug)]
pub struct PostgresQueryExec {
    query: String,
    state: Arc<PostgresAccessState>,
    metrics: ExecutionPlanMetricsSet,
}

impl PostgresQueryExec {
    pub fn new(query: String, state: Arc<PostgresAccessState>) -> Self {
        PostgresQueryExec {
            query,
            state,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for PostgresQueryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        COUNT_SCHEMA.clone()
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
        Err(DataFusionError::Execution(
            "cannot replace children for PostgresQueryExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream = QueryStream {
            state: QueryExecState::Idle,
            opener: QueryOpener {
                query: self.query.clone(),
                state: self.state.clone(),
            },
        };
        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            stream,
            partition,
            &self.metrics,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for PostgresQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PostgresQueryExec(query = {})", self.query)
    }
}

#[derive(Clone)]
struct QueryOpener {
    query: String,
    state: Arc<PostgresAccessState>,
}

impl QueryOpener {
    fn open(&self) -> BoxFuture<'static, Result<u64, tokio_postgres::Error>> {
        let this = self.clone();
        Box::pin(async move { this.state.client.execute(&this.query, &[]).await })
    }
}

enum QueryExecState {
    Idle,
    Open {
        fut: BoxFuture<'static, Result<u64, tokio_postgres::Error>>,
    },
    Done,
    Error,
}

struct QueryStream {
    state: QueryExecState,
    opener: QueryOpener,
}

impl Stream for QueryStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                QueryExecState::Idle => {
                    let fut = self.opener.open();
                    self.state = QueryExecState::Open { fut }
                }
                QueryExecState::Open { fut } => match ready!(fut.poll_unpin(cx)) {
                    Ok(count) => {
                        let record_batch = create_count_record_batch(count);
                        self.state = QueryExecState::Done;
                        return Poll::Ready(Some(Ok(record_batch)));
                    }
                    Err(e) => {
                        self.state = QueryExecState::Error;
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                    }
                },
                QueryExecState::Done | QueryExecState::Error => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for QueryStream {
    fn schema(&self) -> Arc<ArrowSchema> {
        COUNT_SCHEMA.clone()
    }
}
