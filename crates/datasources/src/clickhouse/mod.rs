pub mod errors;

mod stream;

use clickhouse_rs::types::DateTimeType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use errors::{ClickhouseError, Result};
use parking_lot::Mutex;

use async_trait::async_trait;
use clickhouse_rs::{ClientHandle, Options, Pool};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use std::any::Any;
use std::borrow::Cow;
use std::fmt::{self, Display, Write};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::clickhouse::stream::BlockStream;
use crate::common::util;

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

    /// Validate connection to the clickhouse server.
    pub async fn validate_access(&self) -> Result<()> {
        let _state = ClickhouseAccessState::connect(&self.conn_string).await?;
        Ok(())
    }

    /// Validate that we have access to a specific table.
    pub async fn validate_table_access(&self, table_ref: ClickhouseTableRef<'_>) -> Result<()> {
        let state = ClickhouseAccessState::connect(&self.conn_string).await?;
        let _schema = state.get_table_schema(table_ref).await?;
        Ok(())
    }

    pub async fn connect(&self) -> Result<ClickhouseAccessState> {
        ClickhouseAccessState::connect(&self.conn_string).await
    }
}

pub struct ClickhouseAccessState {
    // TODO: We currently limit the pool to 1 connection to have it behave
    // similarly to our other data sources. We will likely want to actually make
    // use of a connection pool to avoid creating connections on every query.
    //
    // A depreceted `connect` method does return us a client direction, unsure
    // if we want to use that or not.
    pool: Pool,
}

impl ClickhouseAccessState {
    async fn connect(conn_str: &str) -> Result<Self> {
        let conn_str = Url::parse(conn_str)?;

        // Verify that the conn_str has "clickhouse://" protocol.
        if conn_str.scheme() != "clickhouse" {
            return Err(ClickhouseError::String(format!(
                "Expected url with scheme `clickhouse://`, got: `{}`",
                conn_str.scheme()
            )));
        }

        let mut secure = false;
        for (key, _val) in conn_str.query_pairs() {
            if key.as_ref() == "s" || key.as_ref() == "secure" {
                secure = true;
                break;
            }
        }

        let addr = {
            let port = if let Some(port) = conn_str.port() {
                port
            } else if secure {
                9440
            } else {
                9000
            };
            if port == 9440 && !secure {
                // We assume that if someone is trying to connect to port 9440,
                // they are trying to connect to the Clickhouse Cloud on the
                // TLS port. Due to a panic, we error early. In a rare scenario,
                // this might be incorrect. So we should fix and push the fix
                // upstream.
                //
                // Issue to track: https://github.com/GlareDB/glaredb/issues/2360
                return Err(ClickhouseError::String(
                    "Cannot connect to SSL/TLS port without secure parameter enabled.
Enable secure param in connection string:
    `clickhouse://<user>:<password>@<host>:<port>/?secure`"
                        .to_string(),
                ));
            }
            let mut addr = if let Some(host) = conn_str.host_str() {
                format!("tcp://default@{host}").parse::<Url>()?
            } else {
                "tcp://default@127.0.0.1".parse::<Url>()?
            };
            addr.set_port(Some(port)).map_err(|_| {
                ClickhouseError::String("unable to set port for clickhouse URL".to_string())
            })?;
            addr
        };

        let mut opts = Options::new(addr)
            .pool_min(1)
            .pool_max(1)
            .secure(secure)
            .ping_timeout(Duration::from_secs(5))
            .connection_timeout(Duration::from_secs(5));

        if let Some(mut path) = conn_str.path_segments() {
            if let Some(database) = path.next() {
                opts = opts.database(database);
            }
        }

        let user = conn_str.username();
        if !user.is_empty() {
            opts = opts.username(user);
        }

        if let Some(password) = conn_str.password() {
            opts = opts.password(password);
        }

        let pool = Pool::new(opts);
        let mut client = pool.get_handle().await?;
        client.ping().await?;

        Ok(ClickhouseAccessState { pool })
    }

    async fn get_table_schema(&self, table_ref: ClickhouseTableRef<'_>) -> Result<ArrowSchema> {
        let mut client = self.pool.get_handle().await?;

        let block = client
            .query(format!("SELECT * FROM {table_ref} LIMIT 0"))
            .fetch_all()
            .await?;

        let mut fields = Vec::with_capacity(block.columns().len());
        for col in block.columns() {
            use clickhouse_rs::types::SqlType;

            /// Convert a clickhouse sql type to an arrow data type.
            ///
            /// Clickhouse type reference: <https://clickhouse.com/docs/en/sql-reference/data-types>
            ///
            /// TODO: Support more types. Note that adding a type here requires
            /// implementing the appropriate conversion in `BlockStream`.
            fn to_data_type(sql_type: &SqlType) -> Result<DataType> {
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
                    // Clickhouse has both a 'Date' type (2 bytes) and a
                    // 'Date32' type (4 bytes). The library doesn't support
                    // 'Date32' type yet.
                    // TODO: Maybe upstream support for Date32?
                    SqlType::Date => DataType::Date32,
                    SqlType::DateTime(DateTimeType::DateTime32) => {
                        DataType::Timestamp(TimeUnit::Nanosecond, None)
                    }
                    SqlType::DateTime(DateTimeType::DateTime64(_precision, tz)) => {
                        // We store all timestamps in nanos, so the precision
                        // here doesn't matter. clickhouse_rs crate handles this
                        // for us.
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.to_string().into()))
                    }
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

#[async_trait]
impl VirtualLister for ClickhouseAccessState {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        let query = "SELECT schema_name FROM information_schema.schemata";

        let mut client = self
            .pool
            .get_handle()
            .await
            .map_err(ExtensionError::access)?;

        let block = client
            .query(query)
            .fetch_all()
            .await
            .map_err(ExtensionError::access)?;

        block
            .rows()
            .map(|row| row.get::<String, usize>(0).map_err(ExtensionError::access))
            .collect::<Result<Vec<_>, ExtensionError>>()
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let query = format!(
            "SELECT table_name FROM information_schema.tables
WHERE table_schema = '{schema}'"
        );

        let mut client = self
            .pool
            .get_handle()
            .await
            .map_err(ExtensionError::access)?;

        let block = client
            .query(query)
            .fetch_all()
            .await
            .map_err(ExtensionError::access)?;

        block
            .rows()
            .map(|row| row.get::<String, usize>(0).map_err(ExtensionError::access))
            .collect::<Result<Vec<_>, ExtensionError>>()
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        let table_ref = ClickhouseTableRef::new(Some(schema), table);
        self.get_table_schema(table_ref)
            .await
            .map(|s| s.fields)
            .map_err(ExtensionError::access)
    }
}

pub struct ClickhouseTableProvider {
    state: Arc<ClickhouseAccessState>,
    table_ref: OwnedClickhouseTableRef,
    schema: Arc<ArrowSchema>,
}

impl ClickhouseTableProvider {
    pub async fn try_new(
        access: ClickhouseAccess,
        table_ref: OwnedClickhouseTableRef,
    ) -> Result<Self> {
        let state = Arc::new(ClickhouseAccessState::connect(&access.conn_string).await?);
        let schema = Arc::new(state.get_table_schema(table_ref.as_ref()).await?);

        Ok(ClickhouseTableProvider {
            state,
            table_ref,
            schema,
        })
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };

        // Get the projected columns, joined by a ','. This will be put in the
        // 'SELECT ...' portion of the query.
        let projection_string = projected_schema
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(",");

        let mut query = format!("SELECT {} FROM {}", projection_string, self.table_ref);

        let predicate_string = {
            exprs_to_predicate_string(filters)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };
        if !predicate_string.is_empty() {
            write!(&mut query, " WHERE {predicate_string}")?;
        }

        if let Some(limit) = limit {
            write!(&mut query, " LIMIT {limit}")?;
        }

        let client =
            self.state.pool.get_handle().await.map_err(|e| {
                DataFusionError::Execution(format!("failed to get client handle: {e}"))
            })?;

        Ok(Arc::new(ClickhouseExec::new(
            projected_schema,
            query,
            client,
        )))
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
    /// Output schema.
    schema: ArrowSchemaRef,
    /// A single-use client handle to clickhouse.
    handle: Mutex<Option<ClientHandle>>,
    /// Query to run against clickhouse.
    query: String,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl ClickhouseExec {
    fn new(schema: ArrowSchemaRef, query: String, handle: ClientHandle) -> ClickhouseExec {
        ClickhouseExec {
            schema,
            handle: Mutex::new(Some(handle)),
            query,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for ClickhouseExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
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
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "only single partition supported".to_string(),
            ));
        }

        // This would need to be updated for if/when we do multiple partitions
        // (1 client handle per partition).
        let client = match self.handle.lock().take() {
            Some(client) => client,
            None => {
                return Err(DataFusionError::Execution(
                    "client handle already taken".to_string(),
                ))
            }
        };

        let stream = BlockStream::execute(client, self.query.clone(), self.schema());

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

impl DisplayAs for ClickhouseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClickhouseExec")
    }
}

impl fmt::Debug for ClickhouseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseExec")
            .field("schema", &self.schema)
            .field("query", &self.query)
            .finish_non_exhaustive()
    }
}

/// Convert filtering expressions to a predicate string usable with the
/// generated Postgres query.
fn exprs_to_predicate_string(exprs: &[Expr]) -> Result<String> {
    let mut ss = Vec::new();
    let mut buf = String::new();
    for expr in exprs {
        if try_write_expr(expr, &mut buf)? {
            ss.push(buf);
            buf = String::new();
        }
    }

    Ok(ss.join(" AND "))
}

/// Try to write the expression to the string, returning true if it was written.
fn try_write_expr(expr: &Expr, buf: &mut String) -> Result<bool> {
    match expr {
        Expr::Column(col) => {
            write!(buf, "{}", col)?;
        }
        Expr::Literal(val) => {
            util::encode_literal_to_text(util::Datasource::Clickhouse, buf, val)?;
        }
        Expr::IsNull(expr) => {
            if try_write_expr(expr, buf)? {
                write!(buf, " IS NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsNotNull(expr) => {
            if try_write_expr(expr, buf)? {
                write!(buf, " IS NOT NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsTrue(expr) => {
            if try_write_expr(expr, buf)? {
                write!(buf, " IS TRUE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsFalse(expr) => {
            if try_write_expr(expr, buf)? {
                write!(buf, " IS FALSE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::BinaryExpr(binary) => {
            if !try_write_expr(binary.left.as_ref(), buf)? {
                return Ok(false);
            }
            write!(buf, " {} ", binary.op)?;
            if !try_write_expr(binary.right.as_ref(), buf)? {
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

#[derive(Debug, Clone)]
pub struct ClickhouseTableRef<'a> {
    database: Option<Cow<'a, str>>,
    table: Cow<'a, str>,
}

impl<'a> ClickhouseTableRef<'a> {
    pub fn new<S, T>(database: Option<S>, table: T) -> Self
    where
        S: Into<Cow<'a, str>>,
        T: Into<Cow<'a, str>>,
    {
        Self {
            database: database.map(Into::into),
            table: table.into(),
        }
    }

    pub fn as_ref(&self) -> ClickhouseTableRef<'_> {
        ClickhouseTableRef {
            database: self.database.as_ref().map(|s| s.as_ref().into()),
            table: self.table.as_ref().into(),
        }
    }
}

pub type OwnedClickhouseTableRef = ClickhouseTableRef<'static>;

impl<'a> Display for ClickhouseTableRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(schema) = self.database.as_ref() {
            write!(f, "{schema}.")?;
        }
        write!(f, "{}", self.table)
    }
}
