pub mod errors;

mod client;

use async_trait::async_trait;
use chrono::naive::NaiveDateTime;
use chrono::{DateTime, Utc};
use client::{Client, QueryStream};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{BinaryExpr, Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use errors::{Result, SqlServerError};
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use tiberius::FromSql;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::warn;

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::common::util;

/// Timeout when attempting to connecting to the remote server.
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

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

    /// Connect to the server and return the access state.
    pub async fn connect(&self) -> Result<SqlServerAccessState> {
        SqlServerAccessState::connect(self.config.clone()).await
    }
}

#[derive(Debug)]
pub struct SqlServerAccessState {
    client: Client,
    /// Handle for underlying sql server connection.
    ///
    /// Kept on struct to avoid dropping the connection.
    _conn_handle: JoinHandle<()>,
}

impl SqlServerAccessState {
    async fn connect(config: tiberius::Config) -> Result<Self> {
        let socket =
            match tokio::time::timeout(CONNECTION_TIMEOUT, TcpStream::connect(config.get_addr()))
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    return Err(SqlServerError::String(format!(
                        "timed out connection to SQL Server after {} seconds",
                        CONNECTION_TIMEOUT.as_secs(),
                    )))
                }
            }?;
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

    /// Get the arrow schema and sql server schema for a table.
    async fn get_table_schema(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<(ArrowSchema, Vec<tiberius::Column>)> {
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
                ColumnType::Bit | ColumnType::Bitn => DataType::Boolean,
                ColumnType::Int1 => DataType::Int8,
                ColumnType::Int2 => DataType::Int16,
                ColumnType::Int4 => DataType::Int32,
                ColumnType::Int8 | ColumnType::Intn => DataType::Int64,
                ColumnType::Float4 => DataType::Float32,
                ColumnType::Float8 | ColumnType::Floatn => DataType::Float64,
                // TODO: Double check that this mapping is correct.
                ColumnType::Datetime
                | ColumnType::Datetime2
                | ColumnType::Datetime4
                | ColumnType::Datetimen => DataType::Timestamp(TimeUnit::Nanosecond, None),
                ColumnType::DatetimeOffsetn => {
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
                }
                ColumnType::Guid => DataType::Utf8,
                // TODO: These actually have UTF-16 encoding...
                ColumnType::Text
                | ColumnType::NChar
                | ColumnType::NText
                | ColumnType::BigChar
                | ColumnType::BigVarChar
                | ColumnType::NVarchar => DataType::Utf8,
                ColumnType::BigBinary | ColumnType::BigVarBin => DataType::Binary,
                other => {
                    return Err(SqlServerError::String(format!(
                        "unsupported SQL Server type: {other:?}"
                    )))
                }
            };

            let field = Field::new(col.name(), arrow_typ, true);
            fields.push(field);
        }

        Ok((ArrowSchema::new(fields), cols.to_vec()))
    }
}

#[async_trait]
impl VirtualLister for SqlServerAccessState {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        let mut query = self
            .client
            .query("SELECT schema_name FROM information_schema.schemata")
            .await
            .map_err(ExtensionError::access)?;

        let mut schema_names = Vec::new();
        while let Some(row) = query.next().await {
            let row = row.map_err(ExtensionError::access)?;
            if let Some(s) = row
                .try_get::<&str, usize>(0)
                .map_err(ExtensionError::access)?
            {
                schema_names.push(s.to_owned());
            }
        }

        Ok(schema_names)
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let mut query = self
            .client
            .query(format!(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            ))
            .await
            .map_err(ExtensionError::access)?;

        let mut table_names = Vec::new();
        while let Some(row) = query.next().await {
            let row = row.map_err(ExtensionError::access)?;
            if let Some(s) = row
                .try_get::<&str, usize>(0)
                .map_err(ExtensionError::access)?
            {
                table_names.push(s.to_owned());
            }
        }

        Ok(table_names)
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        use ExtensionError::ListingErrBoxed;

        let (schema, _) = self
            .get_table_schema(schema, table)
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        Ok(schema.fields)
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
    sql_server_schema: Vec<tiberius::Column>,
}

impl SqlServerTableProvider {
    pub async fn try_new(conf: SqlServerTableProviderConfig) -> Result<Self> {
        let state = SqlServerAccessState::connect(conf.access.config).await?;
        let (arrow_schema, sql_server_schema) =
            state.get_table_schema(&conf.schema, &conf.table).await?;

        Ok(Self {
            schema: conf.schema,
            table: conf.table,
            state: Arc::new(state),
            arrow_schema: Arc::new(arrow_schema),
            sql_server_schema,
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
            Some(limit) => format!("TOP {}", limit),
            None => String::new(),
        };

        let predicate_string = exprs_to_predicate_string(filters, &self.sql_server_schema)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let predicate_string = if predicate_string.is_empty() {
            predicate_string
        } else {
            format!("WHERE {predicate_string}")
        };

        let query = format!(
            "SELECT {limit_string} {projection_string} FROM {}.{} {predicate_string}",
            self.schema, self.table
        );

        eprintln!("query = {query:?}");

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
            "inserts not yet supported for SQL Server".to_string(),
        ))
    }
}

/// Convert filtering expressions to a predicate string usable with the
/// generated SQL Server query.
fn exprs_to_predicate_string(
    exprs: &[Expr],
    sql_server_schema: &[tiberius::Column],
) -> Result<String> {
    let mut ss = Vec::new();
    let mut buf = String::new();

    let dt_map: HashMap<_, _> = sql_server_schema
        .iter()
        .map(|col| (col.name(), col.column_type()))
        .collect();

    for expr in exprs {
        if try_write_expr(expr, &dt_map, &mut buf)? {
            ss.push(buf);
            buf = String::new();
        }
    }

    Ok(ss.join(" AND "))
}

/// Try to write the expression to the string, returning true if it was written.
fn try_write_expr(
    expr: &Expr,
    dt_map: &HashMap<&str, tiberius::ColumnType>,
    buf: &mut String,
) -> Result<bool> {
    match expr {
        Expr::Column(col) => {
            write!(buf, "{}", col)?;
        }
        Expr::Literal(val) => {
            util::encode_literal_to_text(util::Datasource::SqlServer, buf, val)?;
        }
        Expr::IsNull(expr) => {
            if try_write_expr(expr, dt_map, buf)? {
                write!(buf, " IS NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsNotNull(expr) => {
            if try_write_expr(expr, dt_map, buf)? {
                write!(buf, " IS NOT NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsTrue(expr) => {
            if try_write_expr(expr, dt_map, buf)? {
                write!(buf, " = 1")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsFalse(expr) => {
            if try_write_expr(expr, dt_map, buf)? {
                write!(buf, " = 0")?;
            } else {
                return Ok(false);
            }
        }
        Expr::BinaryExpr(binary) => {
            if should_skip_binary_expr(binary, dt_map)? {
                return Ok(false);
            }

            if !try_write_expr(binary.left.as_ref(), dt_map, buf)? {
                return Ok(false);
            }
            write!(buf, " {} ", binary.op)?;
            if !try_write_expr(binary.right.as_ref(), dt_map, buf)? {
                return Ok(false);
            }
        }
        _ => {
            // Unsupported.
            return Ok(false);
        }
    }

    Ok(true)
}

fn should_skip_binary_expr(
    expr: &BinaryExpr,
    dt_map: &HashMap<&str, tiberius::ColumnType>,
) -> Result<bool> {
    fn is_text_col(expr: &Expr, dt_map: &HashMap<&str, tiberius::ColumnType>) -> Result<bool> {
        match expr {
            Expr::Column(col) => {
                let sql_type = dt_map.get(col.name.as_str()).ok_or_else(|| {
                    SqlServerError::String(format!("invalid column `{}`", col.name))
                })?;
                use tiberius::ColumnType;
                Ok(matches!(sql_type, ColumnType::Text | ColumnType::NText))
            }
            _ => Ok(false),
        }
    }

    // Skip if we're trying to do any kind of binary op with text column
    Ok(is_text_col(&expr.left, dt_map)? || is_text_col(&expr.right, dt_map)?)
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
            DataType::Int64 => {
                let mut arr = Int64Builder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<Intn> = row.try_get(col_idx)?;
                    arr.append_option(val.map(|v| v.0));
                }
                Arc::new(arr.finish())
            }
            DataType::Float32 => make_column!(Float32Builder, rows, col_idx),
            DataType::Float64 => {
                let mut arr = Float64Builder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<Floatn> = row.try_get(col_idx)?;
                    arr.append_option(val.map(|v| v.0));
                }
                Arc::new(arr.finish())
            }
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
            dt @ DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
                let mut arr = TimestampNanosecondBuilder::with_capacity(rows.len())
                    .with_data_type(dt.clone());
                for row in rows.iter() {
                    let val: Option<DateTime<Utc>> = row.try_get(col_idx)?;
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

/// Read a variable width integer from a column value.
///
/// SQL Server has the interesting property where a column can store ints of
/// different sizes (so row 'A' might have the column be i32, and row 'B' it
/// might be i64). Since we need to be able store column values all as one type,
/// we just promote everything to i64 when reading.
// TODO: Likely need to do this for the other 'n' types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct Intn(i64);

impl<'a> FromSql<'a> for Intn {
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        Ok(match value {
            tiberius::ColumnData::U8(v) => v.as_ref().map(|v| Intn(*v as i64)),
            tiberius::ColumnData::I16(v) => v.as_ref().map(|v| Intn(*v as i64)),
            tiberius::ColumnData::I32(v) => v.as_ref().map(|v| Intn(*v as i64)),
            tiberius::ColumnData::I64(v) => v.as_ref().map(|v| Intn(*v)),
            other => {
                return Err(tiberius::error::Error::Conversion(
                    format!("{other:?} to Intn").into(),
                ))
            }
        })
    }
}

/// Read a variable width float from a column value.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
#[repr(transparent)]
struct Floatn(f64);

impl<'a> FromSql<'a> for Floatn {
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        Ok(match value {
            tiberius::ColumnData::F32(v) => v.as_ref().map(|v| Floatn(*v as f64)),
            tiberius::ColumnData::F64(v) => v.as_ref().map(|v| Floatn(*v)),
            other => {
                return Err(tiberius::error::Error::Conversion(
                    format!("{other:?} to Floatn").into(),
                ))
            }
        })
    }
}
