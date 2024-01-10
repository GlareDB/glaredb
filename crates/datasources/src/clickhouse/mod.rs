pub mod errors;

mod convert;

use clickhouse_rs::types::DateTimeType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use errors::{ClickhouseError, Result};
use futures::{Stream, StreamExt};
use klickhouse::block::Block;
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
use klickhouse::{Client, ClientOptions, KlickhouseError};
use std::any::Any;
use std::borrow::Cow;
use std::fmt::{self, Display, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use url::Url;

use crate::common::util;

use self::convert::ConvertStream;

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
    client: Client,
}

impl ClickhouseAccessState {
    async fn connect(conn_str: &str) -> Result<Self> {
        let mut conn_str = Url::parse(conn_str)?;

        // Verify that the conn_str has "clickhouse://" protocol.
        if conn_str.scheme() != "clickhouse" {
            return Err(ClickhouseError::String(format!(
                "Expected url with scheme `clickhouse://`, got: `{}`",
                conn_str.scheme()
            )));
        }

        conn_str
            .set_scheme("tcp")
            .expect("tcp should be valid scheme to set");

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

        // Clickhouse sets the service to "idle" when not queried for a while.
        // Waking up the service can sometimes take from 20-30 seconds, hence
        // setting the timeout accordingly.
        //
        // TODO: Actually use this (made unused with the switch to klickhouse).
        let _timeout = Duration::from_secs(30);

        let mut opts = ClientOptions::default();

        if let Some(mut path) = conn_str.path_segments() {
            if let Some(database) = path.next() {
                opts.default_database = database.to_string();
            }
        }

        let user = conn_str.username();
        if !user.is_empty() {
            opts.username = user.to_string();
        }

        if let Some(password) = conn_str.password() {
            opts.username = password.to_string();
        }

        let (read, write) = TcpStream::connect(addr.to_string()).await?.into_split();
        let client = Client::connect_stream(read, write, opts).await?;

        Ok(ClickhouseAccessState { client })
    }

    async fn get_table_schema(&self, table_ref: ClickhouseTableRef<'_>) -> Result<ArrowSchema> {
        let mut stream = self
            .client
            .query_raw(format!("SELECT * FROM {table_ref} LIMIT 0"))
            .await?;

        let block = match stream.next().await {
            Some(block) => block?,
            None => {
                return Err(ClickhouseError::String(format!(
                    "query for '{table_ref}' returned no blocks"
                )))
            }
        };

        convert::block_to_schema(block)
    }
}

#[async_trait]
impl VirtualLister for ClickhouseAccessState {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        unimplemented!()
        // let query = "SELECT schema_name FROM information_schema.schemata";

        // let mut client = self
        //     .pool
        //     .get_handle()
        //     .await
        //     .map_err(ExtensionError::access)?;

        // let block = client
        //     .query(query)
        //     .fetch_all()
        //     .await
        //     .map_err(ExtensionError::access)?;

        // block
        //     .rows()
        //     .map(|row| row.get::<String, usize>(0).map_err(ExtensionError::access))
        //     .collect::<Result<Vec<_>, ExtensionError>>()
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        unimplemented!()
        //         let query = format!(
        //             "SELECT table_name FROM information_schema.tables
        // WHERE table_schema = '{schema}'"
        //         );

        //         let mut client = self
        //             .pool
        //             .get_handle()
        //             .await
        //             .map_err(ExtensionError::access)?;

        //         let block = client
        //             .query(query)
        //             .fetch_all()
        //             .await
        //             .map_err(ExtensionError::access)?;

        //         block
        //             .rows()
        //             .map(|row| row.get::<String, usize>(0).map_err(ExtensionError::access))
        //             .collect::<Result<Vec<_>, ExtensionError>>()
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        unimplemented!()
        // let table_ref = ClickhouseTableRef::new(Some(schema), table);
        // self.get_table_schema(table_ref)
        //     .await
        //     .map(|s| s.fields)
        //     .map_err(ExtensionError::access)
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

        // TODO: This should be happening in the exec.
        let stream = self
            .state
            .client
            .query_raw(query)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        Ok(Arc::new(ClickhouseExec::new(self.schema(), stream)))
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
    /// Stream we're pulling from.
    stream: Mutex<Option<ConvertStream>>,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl ClickhouseExec {
    fn new(
        schema: ArrowSchemaRef,
        stream: impl Stream<Item = Result<Block, KlickhouseError>> + Send + 'static,
    ) -> Self {
        ClickhouseExec {
            schema: schema.clone(),
            stream: Mutex::new(Some(ConvertStream {
                schema,
                inner: Box::pin(stream),
            })),
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
        // (1 convert stream per partition).
        let stream = match self.stream.lock().take() {
            Some(stream) => stream,
            None => {
                return Err(DataFusionError::Execution(
                    "stream already taken".to_string(),
                ))
            }
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

impl DisplayAs for ClickhouseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClickhouseExec")
    }
}

impl fmt::Debug for ClickhouseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseExec")
            .field("schema", &self.schema)
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
