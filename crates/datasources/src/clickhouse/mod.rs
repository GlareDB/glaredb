pub mod errors;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use errors::{ClickhouseError, Result};

use async_trait::async_trait;
use clickhouse_rs::{Block, ClientHandle, Options, Pool};
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
use futures::{Stream, StreamExt};
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
    /// Format: clickhouse://user:password@host:9000/db
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

    async fn get_table_schema(&self, name: &str) -> Result<ArrowSchema> {
        let mut client = self.pool.get_handle().await?;
        // TODO: Does clickhouse actually return blocks for empty data sets?
        let mut blocks = client
            .query(format!("SELECT * FROM {name} LIMIT 0"))
            .stream_blocks();

        let block = match blocks.next().await {
            Some(block) => block?,
            None => {
                return Err(ClickhouseError::String(
                    "unable to determine schema for table, no blocks returned".to_string(),
                ))
            }
        };

        let mut fields = Vec::with_capacity(block.columns().len());
        for col in block.columns() {
            use clickhouse_rs::types::SqlType;

            fn to_data_type(sql_type: &SqlType) -> Result<DataType> {
                // TODO: The rest
                Ok(match sql_type {
                    SqlType::Bool => DataType::Boolean,
                    SqlType::UInt8 => DataType::UInt8,
                    SqlType::UInt16 => DataType::UInt16,
                    SqlType::UInt32 => DataType::UInt32,
                    SqlType::UInt64 => DataType::UInt64,
                    SqlType::Int8 => DataType::Int8,
                    SqlType::Int16 => DataType::Int16,
                    SqlType::Int32 => DataType::Int32,
                    SqlType::Int64 => DataType::Int64,
                    SqlType::Float32 => DataType::Float32,
                    SqlType::Float64 => DataType::Float64,
                    SqlType::String | SqlType::FixedString(_) => DataType::Utf8,
                    other => {
                        return Err(ClickhouseError::String(format!(
                            "unsupported Clickhouse type: {other:?}"
                        )))
                    }
                })
            }

            let (arrow_typ, nullable) = match col.sql_type() {
                SqlType::Nullable(typ) => (to_data_type(typ)?, true),
                typ => (to_data_type(&typ)?, false),
            };

            let field = Field::new(col.name(), arrow_typ, nullable);
            fields.push(field);
        }

        Ok(ArrowSchema::new(fields))
    }
}

pub struct ClickhouseTableProvider {
    state: Arc<ClickhouseAccessState>,
    schema: Arc<ArrowSchema>,
}

impl ClickhouseTableProvider {
    pub async fn try_new(access: ClickhouseAccess, table: &str) -> Result<Self> {
        let state = Arc::new(ClickhouseAccessState::connect(&access.conn_string).await?);
        let schema = Arc::new(state.get_table_schema(table).await?);

        Ok(ClickhouseTableProvider { state, schema })
    }
}

#[async_trait]
impl TableProvider for ClickhouseTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
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
    state: Arc<ClickhouseAccessState>,
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

        let state = self.state.clone();
        let fut = async move {
            let mut handle = state.pool.get_handle().await.unwrap();
            let stream = handle.query("select 1").stream_blocks();
            // whyyyyyyyyyyy
        };

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

enum BlockStreamState {}

/// Converts a block stream from clickhouse into a record batch stream.
struct BlockStream {}

impl BlockStream {}

impl Stream for BlockStream {
    type Item = DatafusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }
}
