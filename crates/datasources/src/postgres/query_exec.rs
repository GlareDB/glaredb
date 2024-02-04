use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::expr::Sort;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    RecordBatchStream,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::{Column, Expr};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};

use super::PostgresAccessState;
use crate::common::util::{create_count_record_batch, COUNT_SCHEMA};

#[derive(Debug)]
pub struct PostgresQueryExec {
    query: String,
    state: Arc<PostgresAccessState>,
    metrics: ExecutionPlanMetricsSet,
    order_by: Option<String>,
    sort_spec: Option<Vec<PhysicalSortExpr>>,
}

impl PostgresQueryExec {
    pub fn new(query: String, state: Arc<PostgresAccessState>, order_by: Option<String>) -> Self {
        let mut obj = PostgresQueryExec {
            query,
            state,
            order_by,
            metrics: ExecutionPlanMetricsSet::new(),
            sort_spec: None,
        };
        obj.sort_spec = obj.build_sort();
        obj
    }

    fn build_sort(&mut self) -> Option<Vec<PhysicalSortExpr>> {
        let stmt = self.order_by.clone()?;

        let parser = datafusion::sql::sqlparser::parser::Parser::new(&GenericDialect);

        let expr = parser
            .try_with_sql(stmt.as_str())
            .ok()?
            .parse_order_by_expr()
            .ok()?;

        let expression = Expr::Sort(Sort {
            expr: Box::new(match expr.expr {
                datafusion::sql::sqlparser::ast::Expr::Identifier(col) => {
                    Some(Expr::Column(Column::from_name(col.value)))
                }
                _ => return None,
            }?),
            asc: expr.asc.unwrap_or(false),
            nulls_first: expr.nulls_first.unwrap_or(false),
        });
        let dfschema = DFSchema::try_from(self.schema().as_ref().to_owned()).ok()?;

        let pxpr = datafusion::physical_expr::create_physical_expr(
            &expression,
            &dfschema,
            &self.schema(),
            &ExecutionProps::new(),
        )
        .ok()?;
        Some(vec![PhysicalSortExpr {
            expr: pxpr,
            options: SortOptions {
                nulls_first: expr.nulls_first.unwrap_or(false),
                descending: !expr.asc.unwrap_or(false),
            },
        }])
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
        self.sort_spec.as_ref().map(|v| v.as_ref())
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

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
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
