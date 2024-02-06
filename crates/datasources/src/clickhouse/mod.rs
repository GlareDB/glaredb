pub mod errors;

mod convert;

use std::any::Any;
use std::borrow::Cow;
use std::fmt::{self, Display, Write};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    Field,
    Fields,
    Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use errors::{ClickhouseError, Result};
use futures::StreamExt;
use klickhouse::{Client, ClientOptions, KlickhouseError};
use rustls::ServerName;
use tokio_rustls::TlsConnector;
use url::Url;

use self::convert::ConvertStream;
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
    pub(crate) client: Client,
    database: String,
}

impl ClickhouseAccessState {
    async fn connect(conn_str: &str) -> Result<Self> {
        let conn_str = Url::parse(conn_str)?;

        // Verify that the conn_str has "clickhouse://" or "tcp://" protocol.
        let scheme = conn_str.scheme();
        if scheme != "clickhouse" && scheme != "tcp" {
            return Err(ClickhouseError::String(format!(
                "Expected url with scheme 'clickhouse://' or 'tcp://', got: `{}`",
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

        let host = {
            let port = if let Some(port) = conn_str.port() {
                port
            } else if secure {
                9440
            } else {
                9000
            };
            let host = conn_str.host_str().unwrap_or("127.0.0.1");
            format!("{host}:{port}")
        };

        // Clickhouse sets the service to "idle" when not queried for a while.
        // Waking up the service can sometimes take from 20-30 seconds, hence
        // setting the timeout accordingly.
        //
        // TODO: Actually use this (made unused with the switch to klickhouse).
        let _timeout = Duration::from_secs(30);

        let mut opts = ClientOptions {
            // Set the default database to "default" if not provided (since
            // `ClientOptions::default` sets it to an empty string).
            default_database: "default".to_string(),
            ..Default::default()
        };

        if let Some(mut path) = conn_str.path_segments() {
            if let Some(database) = path.next() {
                opts.default_database = database.to_string();
            }
        }

        let database = opts.default_database.clone();

        let user = conn_str.username();
        if !user.is_empty() {
            opts.username = user.to_string();
        }

        if let Some(password) = conn_str.password() {
            opts.password = password.to_string();
        }

        let client = if secure {
            // TODO: Some of this stuff might be useful to pull out into a
            // common tls module.
            //
            // TODO: Provide a better error when connecting fails (e.g.
            // authentication failure). Currently the error is 'Error: protocol
            // error: failed to receive blocks from upstream: channel closed'
            let mut root_store = rustls::RootCertStore::empty();

            root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|r| {
                rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                    r.subject.to_vec(),
                    r.subject_public_key_info.to_vec(),
                    r.name_constraints.clone().map(|f| f.to_vec()),
                )
            }));

            let config = Arc::new(
                rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_store)
                    .with_no_client_auth(),
            );

            let server_name = ServerName::try_from(conn_str.host_str().unwrap_or_default())
                .map_err(|e| {
                    ClickhouseError::String(format!(
                        "failed to create server name for {conn_str}: {e}"
                    ))
                })?;

            Client::connect_tls(host, opts, server_name, &TlsConnector::from(config)).await?
        } else {
            Client::connect(host, opts).await?
        };

        Ok(ClickhouseAccessState { client, database })
    }

    async fn get_table_schema(&self, table_ref: ClickhouseTableRef<'_>) -> Result<ArrowSchema> {
        #[derive(Debug, klickhouse::Row)]
        struct ColumnInfo {
            column_name: String,
            data_type: String,
        }

        let table: &str = table_ref.table.as_ref();
        let database = table_ref
            .database
            .as_deref()
            .unwrap_or(self.database.as_str());

        let infos = self
            .client
            .query_collect::<ColumnInfo>(format!(
                "SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = '{table}' AND table_catalog = '{database}'",
            ))
            .await?;

        if infos.is_empty() {
            return Err(ClickhouseError::String(format!(
                "unable to fetch schema: {table_ref} does not exist"
            )));
        }

        let fields = infos
            .into_iter()
            .map(|info| {
                let dt = convert::clickhouse_type_to_arrow_type(&info.data_type)?;
                Ok(Field::new(info.column_name, dt.inner, dt.nullable))
            })
            .collect::<Result<Fields, KlickhouseError>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

#[async_trait]
impl VirtualLister for ClickhouseAccessState {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        let query = "SELECT schema_name FROM information_schema.schemata";

        #[derive(Debug, klickhouse::Row)]
        struct SchemaInfo {
            schema_name: String,
        }

        let names = self
            .client
            .query(query)
            .await
            .map_err(ExtensionError::access)?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|result| result.map(|info: SchemaInfo| info.schema_name))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExtensionError::access)?;

        Ok(names)
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let query = format!(
            "SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{schema}'"
        );

        #[derive(Debug, klickhouse::Row)]
        struct TableInfo {
            table_name: String,
        }

        let names = self
            .client
            .query(query)
            .await
            .map_err(ExtensionError::access)?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|result| result.map(|info: TableInfo| info.table_name))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExtensionError::access)?;

        Ok(names)
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
        let projection_string = if projected_schema.fields().is_empty() {
            "*".to_string()
        } else {
            projected_schema
                .fields
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
                .join(",")
        };

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

        Ok(Arc::new(ClickhouseExec::new(
            projected_schema,
            query,
            self.state.clone(),
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
    /// Stream we're pulling from.
    query: String,
    /// State for querying the external database.
    state: Arc<ClickhouseAccessState>,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl ClickhouseExec {
    fn new(schema: ArrowSchemaRef, query: String, state: Arc<ClickhouseAccessState>) -> Self {
        ClickhouseExec {
            schema,
            query,
            state,
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for ClickhouseExec".to_string(),
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

        let stream =
            ConvertStream::new(self.schema.clone(), self.state.clone(), self.query.clone());

        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            stream,
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
