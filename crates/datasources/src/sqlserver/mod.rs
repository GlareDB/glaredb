pub mod errors;

mod client;

use client::{Client, QueryStream};

use async_trait::async_trait;
use chrono::naive::NaiveDateTime;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use errors::{Result, SqlServerError};
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::warn;

/// Configuration needed for accessing a sql server instance.
pub struct SqlServerAccess {
    config: tiberius::Config,
}

impl SqlServerAccess {
    /// Create access configuration from an ADO formatted connection string.
    ///
    /// ADO connection strings: <https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings>
    /// Example: "server=tcp:localhost,1433;user=SA;password=<YourStrong@Passw0rd>;IntegratedSecurity=true;TrustServerCertificate=true"
    pub fn try_new_from_ado_string(conn_str: &str) -> Result<Self> {
        let config = tiberius::Config::from_ado_string(conn_str)?;
        Ok(Self { config })
    }

    /// Validate that we can connect to server.
    pub async fn validate_access(&self) -> Result<()> {
        let _state = SqlServerAccessState::connect(self.config.clone()).await?;
        Ok(())
    }

    /// Validate that we can connect to a specific table.
    pub async fn validate_table_access(&self, schema: &str, table: &str) -> Result<()> {
        let state = SqlServerAccessState::connect(self.config.clone()).await?;
        let _schema = state.get_table_schema(schema, table).await?;
        Ok(())
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
            .query(format!("SELECT * FROM {schema}.{name} WHERE 1=0"))
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

            // Tiberius' mapping of Rust types to SQL Server types:
            // <https://docs.rs/tiberius/latest/tiberius/trait.FromSql.html>

            let arrow_typ = match col.column_type() {
                ColumnType::Null => DataType::Null,
                ColumnType::Bit => DataType::Boolean,
                ColumnType::Int1 => DataType::Int8,
                ColumnType::Int2 => DataType::Int16,
                // TODO: We don't get n but from my testing, creating a table
                // with INT (32-bit) returns the column type as Intn.
                ColumnType::Int4 | ColumnType::Intn => DataType::Int32,
                ColumnType::Int8 => DataType::Int64,
                ColumnType::Float4 => DataType::Float32,
                ColumnType::Float8 | ColumnType::Floatn => DataType::Float64,
                // TODO: Double check that this mapping is correct.
                ColumnType::Datetime
                | ColumnType::Datetime2
                | ColumnType::Datetime4
                | ColumnType::Datetimen => DataType::Timestamp(TimeUnit::Nanosecond, None),
                // TODO: Tiberius doesn't give us the offset here.
                ColumnType::DatetimeOffsetn => DataType::Timestamp(TimeUnit::Nanosecond, None),
                ColumnType::Guid => DataType::Utf8,
                // TODO: These actually have UTF-16 encoding...
                ColumnType::Text
                | ColumnType::NChar
                | ColumnType::NText
                | ColumnType::BigChar
                | ColumnType::NVarchar => DataType::Utf8,
                ColumnType::BigBinary => DataType::Binary,
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
        _filters: &[Expr],
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

        // TODO: Where/filters

        let query = format!(
            "SELECT {projection_string} FROM {}.{} {limit_string}",
            self.schema, self.table
        );

        Ok(Arc::new(SqlServerExec {
            query,
            state: self.state.clone(),
            arrow_schema: projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "inserts not supported for SQL Server".to_string(),
        ))
    }
}

/// Execution plan for reading from SQL Server.
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

                RowStreamState::Scan { stream } => match ready!(stream.poll_next_unpin(cx)) {
                    Some(chunk) => match rows_to_record_batch(chunk, self.arrow_schema.clone()) {
                        Ok(batch) => return Poll::Ready(Some(Ok(batch))),
                        Err(e) => {
                            self.stream_state = RowStreamState::Done;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                    },
                    None => self.stream_state = RowStreamState::Done,
                },

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

/// Convert a chunk of rows into a record batch.
fn rows_to_record_batch(
    rows: Vec<Result<tiberius::Row>>,
    schema: ArrowSchemaRef,
) -> Result<RecordBatch> {
    use datafusion::arrow::array::{
        Array, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
    };

    let rows = rows.into_iter().collect::<Result<Vec<_>>>()?;

    /// Macro for generating the match arms when converting rows to a record batch.
    macro_rules! make_column {
        ($builder:ty, $rows:expr, $col_idx:expr) => {{
            let mut arr = <$builder>::with_capacity($rows.len());
            for row in $rows.iter() {
                arr.append_option(row.try_get($col_idx)?);
            }
            Arc::new(arr.finish())
        }};
    }

    let mut columns = Vec::with_capacity(schema.fields.len());
    for (col_idx, field) in schema.fields.iter().enumerate() {
        let col: Arc<dyn Array> = match field.data_type() {
            DataType::Boolean => make_column!(BooleanBuilder, rows, col_idx),
            DataType::Int16 => make_column!(Int16Builder, rows, col_idx),
            DataType::Int32 => make_column!(Int32Builder, rows, col_idx),
            DataType::Int64 => make_column!(Int64Builder, rows, col_idx),
            DataType::Float32 => make_column!(Float32Builder, rows, col_idx),
            DataType::Float64 => make_column!(Float64Builder, rows, col_idx),
            DataType::Utf8 => {
                // Assumes an average of 16 bytes per item.
                let mut arr = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows.iter() {
                    let val: Option<&str> = row.try_get(col_idx)?;
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Binary => {
                // Assumes an average of 16 bytes per item.
                let mut arr = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows.iter() {
                    let val: Option<&[u8]> = row.try_get(col_idx)?;
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let mut arr = TimestampNanosecondBuilder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<NaiveDateTime> = row.try_get(col_idx)?;
                    let val = val.map(|v| v.timestamp_nanos_opt().unwrap());
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }

            // TODO: All the others...
            // Tiberius mapping: <https://docs.rs/tiberius/latest/tiberius/trait.FromSql.html>
            other => {
                return Err(SqlServerError::String(format!(
                    "unsupported data type for sql server: {other}"
                )))
            }
        };
        columns.push(col);
    }

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}
