pub mod errors;

mod client;

use client::{Client, Connection, QueryStream};

use async_trait::async_trait;
use chrono::naive::{NaiveDateTime, NaiveTime};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use datafusion::arrow::array::Decimal128Builder;
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use errors::{Result, SqlServerError};
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use parking_lot::Mutex;
use protogen::metastore::types::options::TunnelOptions;
use protogen::{FromOptionalField, ProtoConvError};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::borrow::{Borrow, Cow};
use std::fmt::{self, Write};
use std::pin::Pin;
use std::process::Output;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::warn;

pub struct SqlServerAccess {
    config: tiberius::Config,
}

impl SqlServerAccess {
    pub fn try_new_from_ado_string(conn_str: &str) -> Result<Self> {
        let config = tiberius::Config::from_ado_string(conn_str)?;
        Ok(Self { config })
    }
}

#[derive(Debug)]
struct SqlServerAccessState {
    client: Client,
    /// Handle for underlying sql server connection.
    ///
    /// Kept on struct to avoid dropping the connection.
    _conn_handle: JoinHandle<()>,
}

impl SqlServerAccessState {
    async fn connect(config: tiberius::Config) -> Result<Self> {
        let socket = TcpStream::connect(config.get_addr()).await?;
        socket.set_nodelay(true)?;
        let (client, connection) = client::connect(config, socket.compat_write()).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = connection.run().await {
                warn!(%e, "sql server connection errored");
            }
        });

        Ok(SqlServerAccessState {
            client,
            _conn_handle: handle,
        })
    }

    /// Get the arrow schema for a table.
    async fn get_table_schema(&self, schema: &str, name: &str) -> Result<ArrowSchema> {
        let mut query = self
            .client
            .query(format!("SELECT * FROM {schema}.{name} WHERE false"))
            .await?;
        let cols = query.columns().await?;

        let cols = match cols {
            Some(cols) => cols,
            None => {
                return Err(SqlServerError::String(
                    "unable to determine schema for table, query returned no columns".to_string(),
                ))
            }
        };

        let mut fields = Vec::with_capacity(cols.len());
        for col in cols {
            use tiberius::ColumnType;

            // TODO: Decimal/Numeric, tiberius doesn't seem to provide the
            // scale/precision.

            let arrow_typ = match col.column_type() {
                ColumnType::Null => DataType::Null,
                ColumnType::Bit => DataType::Binary,
                ColumnType::Int1 => DataType::Int8,
                ColumnType::Int2 => DataType::Int16,
                ColumnType::Int4 => DataType::Int32,
                ColumnType::Int8 => DataType::Int64,
                ColumnType::Float4 => DataType::Float32,
                ColumnType::Float8 => DataType::Float64,
                ColumnType::Datetime4 => DataType::Date32,
                ColumnType::Guid => DataType::Utf8,
                // TODO: These actually have UTF-16 encoding...
                ColumnType::Text
                | ColumnType::NChar
                | ColumnType::NText
                | ColumnType::BigChar
                | ColumnType::NVarchar => DataType::Utf8,
                other => {
                    return Err(SqlServerError::String(format!(
                        "unsupported SQL Server type: {other:?}"
                    )))
                }
            };

            let field = Field::new(col.name(), arrow_typ, true);
            fields.push(field);
        }

        Ok(ArrowSchema::new(fields))
    }
}

pub struct SqlServerTableProviderConfig {
    pub access: SqlServerAccess,
    pub schema: String,
    pub table: String,
}

pub struct SqlServerTableProvider {
    schema: String,
    table: String,
    state: Arc<SqlServerAccessState>,
    arrow_schema: ArrowSchemaRef,
}

impl SqlServerTableProvider {
    pub async fn try_new(conf: SqlServerTableProviderConfig) -> Result<Self> {
        let state = SqlServerAccessState::connect(conf.access.config).await?;
        let arrow_schema = state.get_table_schema(&conf.schema, &conf.table).await?;

        Ok(Self {
            schema: conf.schema,
            table: conf.table,
            state: Arc::new(state),
            arrow_schema: Arc::new(arrow_schema),
        })
    }
}

#[async_trait]
impl TableProvider for SqlServerTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // Project the schema.
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        // Get the projected columns, joined by a ','. This will be put in the
        // 'SELECT ...' portion of the query.
        let projection_string = projected_schema
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(",");

        let limit_string = match limit {
            Some(limit) => format!("LIMIT {}", limit),
            None => String::new(),
        };

        // TODO: Where

        let query = format!(
            "SELECT {projection_string} FROM {}.{} {limit_string}",
            self.schema, self.table
        );

        unimplemented!()
        // Ok(Arc::new(SqlServerExec {
        //     query,
        //     arrow_schema: projected_schema,
        //     metrics: ExecutionPlanMetricsSet::new(),
        // }))
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "inserts not supported for SQL Server".to_string(),
        ))
    }
}

struct SqlServerExec {
    query: String,
    state: Arc<SqlServerAccessState>,
    arrow_schema: ArrowSchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for SqlServerExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
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
            "cannot replace children for SqlServerExec".to_string(),
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

        // Clones to ensure the future is static.
        let query = self.query.clone();
        let state = self.state.clone();
        let fut = async move { state.client.query(query).await };

        let stream = RowStream {
            stream_state: RowStreamState::Opening {
                opening_fut: Box::pin(fut),
            },
            arrow_schema: self.arrow_schema.clone(),
            chunk_size: context.session_config().batch_size(),
        };

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for SqlServerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqlServerExec")
    }
}

impl fmt::Debug for SqlServerExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlServerExec")
            .field("query", &self.query)
            .field("arrow_schema", &self.arrow_schema)
            .finish_non_exhaustive()
    }
}

/// Stream state.
///
/// Transitions:
/// Opening -> Scan
/// Scan -> Done
enum RowStreamState {
    /// We're still opening the query stream.
    Opening {
        opening_fut: BoxFuture<'static, Result<QueryStream>>,
    },
    /// Actively streaming from the query stream.
    Scan {
        stream: BoxStream<'static, Vec<Result<tiberius::Row>>>,
    },
    /// We finished streaming or hit an error.
    Done,
}

struct RowStream {
    /// Current state of the stream.
    stream_state: RowStreamState,
    /// Schema of the output results.
    arrow_schema: ArrowSchemaRef,
    /// Configuration paramter for how large the batches should be.
    chunk_size: usize,
}

impl RowStream {
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DatafusionResult<RecordBatch>>> {
        loop {
            match &mut self.stream_state {
                RowStreamState::Opening { opening_fut } => {
                    match ready!(opening_fut.poll_unpin(cx)) {
                        Ok(stream) => {
                            // We have the stream, advance state.
                            let stream = Box::pin(stream.chunks(self.chunk_size));
                            self.stream_state = RowStreamState::Scan { stream };
                            continue;
                        }
                        Err(e) => {
                            self.stream_state = RowStreamState::Done;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                    };
                }

                RowStreamState::Scan { stream } => {
                    match ready!(stream.poll_next_unpin(cx)) {
                        Some(chunk) => {
                            // TODO: Process chunks
                        }
                        None => self.stream_state = RowStreamState::Done,
                    }
                }

                RowStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl Stream for RowStream {
    type Item = DatafusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
    }
}

impl RecordBatchStream for RowStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }
}
