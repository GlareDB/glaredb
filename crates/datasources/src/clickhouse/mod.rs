pub mod errors;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use errors::{ClickhouseError, Result};

use async_trait::async_trait;
use clickhouse_rs::{Block, Options, Pool};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::Stream;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use url::Url;

#[derive(Debug, Clone)]
pub struct ClickhouseAccess {
    conn_string: String,
}

impl ClickhouseAccess {
    /// Create access configuration from a connection string.
    ///
    /// Format: tcp://user:password@host:9000/db
    pub fn new_from_connection_string(conn_string: String) -> Self {
        ClickhouseAccess { conn_string }
    }
}

struct ClickhouseAccessState {
    // TODO: We currently limit the pool to 1 connection to have it behave
    // similarly to our other data sources. We will likely want to actually make
    // use of a connection pool to avoid creating connections on every query.
    pool: Pool,
}

impl ClickhouseAccessState {
    async fn connect(conn_str: &str) -> Result<Self> {
        let pool = Pool::new(Options::new(Url::parse(conn_str)?).pool_min(1).pool_max(1));
        let mut client = pool.get_handle().await?;
        client.ping().await?;

        Ok(ClickhouseAccessState { pool })
    }
}

pub struct ClickhouseTableProvider {
    state: Arc<ClickhouseAccessState>,
}

impl ClickhouseTableProvider {}

#[async_trait]
impl TableProvider for ClickhouseTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        unimplemented!()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DatafusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "inserts not yet supported for Clickhouse".to_string(),
        ))
    }
}

struct ClickhouseExec {
    metrics: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for ClickhouseExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        unimplemented!()
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
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for ClickhouseExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "only single partition supported".to_string(),
            ));
        }

        unimplemented!()
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for ClickhouseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClickhouseExec")
    }
}

impl fmt::Debug for ClickhouseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseExec").finish_non_exhaustive()
    }
}

/// Converts a block stream from clickhouse into a record batch stream.
struct BlockStream {}

impl BlockStream {}

impl Stream for BlockStream {
    type Item = DatafusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }
}
