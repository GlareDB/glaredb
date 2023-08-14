pub mod errors;

mod tls;

use crate::common::errors::DatasourceCommonError;
use crate::common::listing::VirtualLister;
use crate::common::ssh::session::SshTunnelSession;
use crate::common::ssh::{key::SshKey, session::SshTunnelAccess};
use crate::common::util;
use async_trait::async_trait;
use chrono::naive::{NaiveDateTime, NaiveTime};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use datafusion::arrow::array::Decimal128Builder;
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
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use errors::{PostgresError, Result};
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use protogen::metastore::types::options::TunnelOptions;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::borrow::{Borrow, Cow};
use std::fmt::{self, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_postgres::binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream};
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::types::{FromSql, Type as PostgresType};
use tokio_postgres::{Client, Config, Connection, CopyOutStream, NoTls, Socket};
use tracing::{debug, warn};

#[derive(Debug)]
pub enum PostgresDbConnection {
    ConnectionString(String),
    Parameters {
        host: String,
        port: Option<u16>,
        user: String,
        password: Option<String>,
        database: String,
    },
}

impl PostgresDbConnection {
    pub fn connection_string(&self) -> String {
        match self {
            Self::ConnectionString(s) => s.to_owned(),
            Self::Parameters {
                host,
                port,
                user,
                password,
                database,
            } => {
                let mut conn_str = String::from("postgres://");
                // Credentials
                write!(&mut conn_str, "{user}").unwrap();
                if let Some(password) = password {
                    write!(&mut conn_str, ":{password}").unwrap();
                }
                // Address
                write!(&mut conn_str, "@{host}").unwrap();
                if let Some(port) = port {
                    write!(&mut conn_str, ":{port}").unwrap();
                }
                // Database
                write!(&mut conn_str, "/{database}").unwrap();
                conn_str
            }
        }
    }
}

/// Information needed for accessing an external Postgres table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresTableAccess {
    /// The schema the table belongs to within postgres.
    pub schema: String,
    /// The table or view name inside of postgres.
    pub name: String,
}

#[derive(Debug)]
pub struct PostgresAccessor {
    /// The Postgres client.
    client: tokio_postgres::Client,
    /// Handle for the underlying Postgres connection.
    /// Also contains the `Session` for the underlying ssh tunnel
    ///
    /// Kept on struct to avoid dropping the postgres connection future and ssh tunnel.
    #[allow(dead_code)]
    conn_handle: JoinHandle<()>,
}

impl PostgresAccessor {
    /// Connect to a postgres instance.
    pub async fn connect(connection_string: &str, tunnel: Option<TunnelOptions>) -> Result<Self> {
        let (client, conn_handle) = Self::connect_internal(connection_string, tunnel).await?;

        Ok(PostgresAccessor {
            client,
            conn_handle,
        })
    }

    async fn connect_internal(
        connection_string: &str,
        tunnel: Option<TunnelOptions>,
    ) -> Result<(Client, JoinHandle<()>)> {
        match tunnel {
            None => Self::connect_direct(connection_string).await,
            Some(TunnelOptions::Ssh(ssh_options)) => {
                let keypair = SshKey::from_bytes(&ssh_options.ssh_key)?;
                let tunnel_access = SshTunnelAccess {
                    connection_string: ssh_options.connection_string,
                    keypair,
                };
                Self::connect_with_ssh_tunnel(connection_string, &tunnel_access).await
            }
            Some(opt) => Err(PostgresError::UnsupportedTunnel(opt.to_string())),
        }
    }

    async fn connect_direct(connection_string: &str) -> Result<(Client, JoinHandle<()>)> {
        let config: Config = connection_string.parse()?;

        fn spawn_conn<T: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
            conn: Connection<Socket, T>,
        ) -> JoinHandle<()> {
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    warn!(%e, "postgres connection errored");
                }
            })
        }

        // Configure tls depending on ssl mode.
        //
        // - Disable => no tls
        // - Prefer => try tls, fallback to no tls on connection error
        // - Require => try tls, no fallback
        // - Any other => return unsupported
        let (client, handle) = match config.get_ssl_mode() {
            SslMode::Disable => {
                let (client, conn) = tokio_postgres::connect(connection_string, NoTls).await?;
                let handle = spawn_conn(conn);
                (client, handle)
            }
            SslMode::Prefer => {
                match tokio_postgres::connect(connection_string, tls::MakeRustlsConnect::default())
                    .await
                {
                    Ok((client, conn)) => {
                        let handle = spawn_conn(conn);
                        (client, handle)
                    }
                    Err(e) => {
                        // The tokio postgres lib does discriminate between tls
                        // and other errors, but that's not made public.
                        debug!(%e, "pg conn falling back to no tls");
                        let (client, conn) =
                            tokio_postgres::connect(connection_string, NoTls).await?;
                        let handle = spawn_conn(conn);
                        (client, handle)
                    }
                }
            }
            SslMode::Require => {
                let (client, conn) =
                    tokio_postgres::connect(connection_string, tls::MakeRustlsConnect::default())
                        .await?;
                let handle = spawn_conn(conn);
                (client, handle)
            }
            other => return Err(PostgresError::UnsupportSslMode(other)),
        };

        Ok((client, handle))
    }

    async fn connect_with_ssh_tunnel(
        connection_string: &str,
        ssh_tunnel: &SshTunnelAccess,
    ) -> Result<(Client, JoinHandle<()>)> {
        let config: Config = connection_string.parse()?;

        let postgres_host = config
            .get_hosts()
            .iter()
            .find_map(|host| match host {
                Host::Tcp(host) => Some(host.clone()),
                _ => None,
            })
            .ok_or(PostgresError::InvalidPgHosts(config.get_hosts().to_vec()))?;

        let postgres_port = config.get_ports().iter().next().cloned().unwrap_or(5432);

        // Open ssh tunnel
        let (session, tunnel_addr) = ssh_tunnel
            .create_tunnel(&(postgres_host, postgres_port))
            .await?;

        fn spawn_conn<T: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
            conn: Connection<TcpStream, T>,
            session: SshTunnelSession,
        ) -> JoinHandle<()> {
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    warn!(%e, "postgres connection errored");
                }
                // If postgres connection is complete, close ssh tunnel
                if let Err(e) = session.close().await {
                    warn!(%e, "closing ssh tunnel errored");
                }
            })
        }

        let tcp_stream = TcpStream::connect(tunnel_addr).await?;

        // Rust doesn't feel like type inferring this for us.
        let tls_connect = <tls::MakeRustlsConnect as MakeTlsConnect<TcpStream>>::make_tls_connect(
            &mut tls::MakeRustlsConnect::default(),
            // TODO: Which host do we want to specify? Since this is being tunneled
            // over ssh, I don't know if SNI actually matters.
            "",
        )?;

        // Configure tls depending on ssl mode.
        //
        // - Disable => no tls
        // - Prefer => try tls, fallback to no tls on connection error
        // - Require => try tls, no fallback
        // - Any other => return unsupported
        let (client, handle) = match config.get_ssl_mode() {
            SslMode::Disable => {
                let (client, conn) = config.connect_raw(tcp_stream, NoTls).await?;
                let handle = spawn_conn(conn, session);
                (client, handle)
            }
            SslMode::Prefer => {
                match config.connect_raw(tcp_stream, tls_connect).await {
                    Ok((client, conn)) => {
                        let handle = spawn_conn(conn, session);
                        (client, handle)
                    }
                    Err(e) => {
                        // The tokio postgres lib does discriminate between tls
                        // and other errors, but that's not made public.
                        debug!(%e, "pg conn falling back to no tls");
                        // Reconnect to get a fresh stream.
                        let tcp_stream = TcpStream::connect(tunnel_addr).await?;
                        let (client, conn) = config.connect_raw(tcp_stream, NoTls).await?;
                        let handle = spawn_conn(conn, session);
                        (client, handle)
                    }
                }
            }
            SslMode::Require => {
                let (client, conn) = config.connect_raw(tcp_stream, tls_connect).await?;
                let handle = spawn_conn(conn, session);
                (client, handle)
            }
            other => return Err(PostgresError::UnsupportSslMode(other)),
        };

        Ok((client, handle))
    }

    /// Validate postgres external database
    pub async fn validate_external_database(
        connection_string: &str,
        tunnel: Option<TunnelOptions>,
    ) -> Result<()> {
        let (client, _) = Self::connect_internal(connection_string, tunnel).await?;

        client.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    /// Validate postgres connection and access to table and retrieve arrow schema
    pub async fn validate_table_access(
        connection_string: &str,
        access: &PostgresTableAccess,
        tunnel: Option<TunnelOptions>,
    ) -> Result<ArrowSchema> {
        let (client, _) = Self::connect_internal(connection_string, tunnel).await?;

        let query = format!(
            "SELECT * FROM {}.{} where false",
            access.schema, access.name
        );
        client.execute(query.as_str(), &[]).await?;

        let (arrow_schema, _) =
            Self::get_table_schema(&client, &access.schema, &access.name).await?;
        Ok(arrow_schema)
    }

    async fn get_table_schema(
        client: &tokio_postgres::Client,
        schema: &str,
        name: &str,
    ) -> Result<(ArrowSchema, Vec<PostgresType>)> {
        // TODO: Get schema using `information_schema.columns`. You can get
        // `numeric_precision` and `numeric_scale` as well from there so you
        // don't have to guess.

        // Get oid of table, and approx number of pages for the relation.
        let mut rows = client
            .query(
                "
SELECT
    pg_class.oid,
    GREATEST(relpages, 1)
FROM pg_class INNER JOIN pg_namespace ON relnamespace = pg_namespace.oid
WHERE nspname=$1 AND relname=$2;
",
                &[&schema, &name],
            )
            .await?;
        // Should only return 0 or 1 row. If 0 rows, then table/schema doesn't
        // exist.
        let row = match rows.pop() {
            Some(row) => row,
            None => {
                return Err(PostgresError::QueryError(format!(
                    "failed to find table: '{schema}.{name}'"
                )))
            }
        };
        let oid: u32 = row.try_get(0)?;

        // TODO: Get approx pages to allow us to calculate number of pages to
        // scan per thread once we do parallel scanning.
        // let approx_pages: i64 = row.try_get(1)?;

        // Get table schema.
        let rows = client
            .query(
                "
SELECT
    attname,
    pg_type.oid
FROM pg_attribute
    INNER JOIN pg_type ON atttypid=pg_type.oid
WHERE attrelid=$1 AND attnum > 0
ORDER BY attnum;
",
                &[&oid],
            )
            .await?;

        let mut names: Vec<String> = Vec::with_capacity(rows.len());
        let mut type_oids: Vec<u32> = Vec::with_capacity(rows.len());
        for row in rows {
            names.push(row.try_get(0)?);
            type_oids.push(row.try_get(1)?);
        }

        let pg_types = type_oids
            .iter()
            .map(|oid| PostgresType::from_oid(*oid))
            .collect::<Option<Vec<_>>>()
            .ok_or(PostgresError::UnknownPostgresOids(type_oids))?;

        let arrow_schema = try_create_arrow_schema(names, &pg_types)?;
        Ok((arrow_schema, pg_types))
    }

    pub async fn into_table_provider(
        self,
        access: PostgresTableAccess,
        predicate_pushdown: bool,
    ) -> Result<PostgresTableProvider> {
        // Every operation in this accessor will happen in a single transaction.
        // The transaction will remain open until the end of the table scan.
        self.client
            .execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
                &[],
            )
            .await?;

        let (arrow_schema, pg_types) =
            Self::get_table_schema(&self.client, &access.schema, &access.name).await?;

        Ok(PostgresTableProvider {
            predicate_pushdown,
            table_access: access,
            accessor: Arc::new(self),
            arrow_schema: Arc::new(arrow_schema),
            pg_types: Arc::new(pg_types),
        })
    }
}

#[async_trait]
impl VirtualLister for PostgresAccessor {
    async fn list_schemas(&self) -> Result<Vec<String>, DatasourceCommonError> {
        use DatasourceCommonError::ListingErrBoxed;

        let rows = self
            .client
            .query("SELECT schema_name FROM information_schema.schemata", &[])
            .await
            .map_err(|e| ListingErrBoxed(Box::new(PostgresError::from(e))))?;

        let mut schema_names: Vec<String> = Vec::with_capacity(rows.len());
        for row in rows {
            schema_names.push(
                row.try_get(0)
                    .map_err(|e| ListingErrBoxed(Box::new(PostgresError::from(e))))?,
            )
        }

        Ok(schema_names)
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, DatasourceCommonError> {
        use DatasourceCommonError::ListingErrBoxed;

        let rows = self
            .client
            .query(
                "
SELECT table_name
FROM information_schema.tables
WHERE
    table_schema = $1
                ",
                &[&schema],
            )
            .await
            .map_err(|e| ListingErrBoxed(Box::new(PostgresError::from(e))))?;

        let mut virtual_tables = Vec::with_capacity(rows.len());
        for row in rows {
            let table = row
                .try_get(0)
                .map_err(|e| ListingErrBoxed(Box::new(PostgresError::from(e))))?;

            virtual_tables.push(table);
        }
        Ok(virtual_tables)
    }
}

pub struct PostgresTableProvider {
    predicate_pushdown: bool,
    table_access: PostgresTableAccess,
    accessor: Arc<PostgresAccessor>,
    arrow_schema: ArrowSchemaRef,
    pg_types: Arc<Vec<PostgresType>>,
}

#[async_trait]
impl TableProvider for PostgresTableProvider {
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

        // Project the postgres types so that it matches the ouput schema.
        let projected_types = match projection {
            Some(projection) => Arc::new(
                projection
                    .iter()
                    .map(|i| self.pg_types[*i].clone())
                    .collect::<Vec<_>>(),
            ),
            None => self.pg_types.clone(),
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

        // Build WHERE clause if predicate pushdown enabled.
        //
        // TODO: This may produce an invalid clause. We'll likely only want to
        // convert some predicates.
        let predicate_string = {
            if self.predicate_pushdown {
                exprs_to_predicate_string(filters)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                String::new()
            }
        };

        // Build copy query.
        let query = format!(
            "COPY (SELECT {} FROM {}.{} {} {} {}) TO STDOUT (FORMAT binary)",
            projection_string,        // SELECT <str>
            self.table_access.schema, // FROM <schema>
            self.table_access.name,   // .<table>
            // [WHERE]
            if predicate_string.is_empty() {
                ""
            } else {
                "WHERE "
            },
            predicate_string.as_str(), // <where-predicate>
            limit_string,              // [LIMIT ..]
        );

        let opener = StreamOpener {
            copy_query: query,
            accessor: self.accessor.clone(),
        };

        Ok(Arc::new(BinaryCopyExec {
            predicate: predicate_string,
            table_access: self.table_access.clone(),
            pg_types: projected_types,
            arrow_schema: projected_schema,
            opener,
        }))
    }
}

/// Copy data from the source Postgres table using the binary copy protocol.
struct BinaryCopyExec {
    predicate: String,
    table_access: PostgresTableAccess,
    pg_types: Arc<Vec<PostgresType>>,
    arrow_schema: ArrowSchemaRef,
    opener: StreamOpener,
}

impl ExecutionPlan for BinaryCopyExec {
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
            "cannot replace children for BinaryCopyExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let stream = ChunkStream {
            state: StreamState::Idle,
            types: self.pg_types.clone(),
            opener: self.opener.clone(),
            arrow_schema: self.arrow_schema.clone(),
        };
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for BinaryCopyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BinaryCopyExec: schema={}, name={}, predicate={}",
            self.table_access.schema,
            self.table_access.name,
            if self.predicate.is_empty() {
                "None"
            } else {
                self.predicate.as_str()
            }
        )
    }
}

impl fmt::Debug for BinaryCopyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinaryCopyExec")
            .field("pg_types", &self.pg_types)
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

/// Open a copy stream.
#[derive(Clone)]
struct StreamOpener {
    /// Query used to initiate the binary copy.
    copy_query: String,
    accessor: Arc<PostgresAccessor>,
}

impl StreamOpener {
    /// Build a future that returns the copy stream.
    fn open(&self) -> BoxFuture<'static, Result<CopyOutStream, tokio_postgres::Error>> {
        let query = self.copy_query.clone();
        let accessor = self.accessor.clone();
        Box::pin(async move {
            let query = query;
            accessor.client.copy_out(&query).await
        })
    }
}

/// Stream state.
///
/// Transitions:
/// Idle -> Open
/// Open -> Scan
/// Scan -> Done
///
/// 'Open' or 'Scan' may also transition to the 'Error' state. The stream is
/// complete once it has reached either 'Done' or 'Error'.
enum StreamState {
    /// Initial state.
    Idle,
    /// Open the copy stream.
    Open {
        fut: BoxFuture<'static, Result<CopyOutStream, tokio_postgres::Error>>,
    },
    /// Binary copy scan ongoing.
    Scan {
        stream: BoxStream<'static, Vec<Result<BinaryCopyOutRow, tokio_postgres::Error>>>,
    },
    /// Scan finished.
    Done,
    /// Scan encountered an error.
    Error,
}

struct ChunkStream {
    /// The currently state of the stream.
    state: StreamState,
    /// Postgres types we're scanning from the binary copy stream.
    types: Arc<Vec<PostgresType>>,
    /// Opens the copy stream.
    opener: StreamOpener,
    /// Schema of the resulting record batch.
    arrow_schema: ArrowSchemaRef,
}

impl Stream for ChunkStream {
    type Item = DatafusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::Idle => {
                    let fut = self.opener.open();
                    self.state = StreamState::Open { fut };
                }
                StreamState::Open { fut } => match ready!(fut.poll_unpin(cx)) {
                    Ok(stream) => {
                        // Get the binary stream from postgres.
                        let stream = BinaryCopyOutStream::new(stream, &self.types);
                        // Chunk the rows. We'll be returning a single record
                        // batch per chunk.
                        let chunked = stream.chunks(1000); // TODO: Make configurable.
                        self.state = StreamState::Scan {
                            stream: chunked.boxed(),
                        };
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                    }
                },
                StreamState::Scan { stream } => match ready!(stream.poll_next_unpin(cx)) {
                    Some(rows) => {
                        match binary_rows_to_record_batch(rows, self.arrow_schema.clone()) {
                            Ok(batch) => {
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            Err(e) => {
                                self.state = StreamState::Error;
                                return Poll::Ready(Some(Err(DataFusionError::External(
                                    Box::new(e),
                                ))));
                            }
                        }
                    }
                    None => {
                        self.state = StreamState::Done;
                    }
                },
                StreamState::Done | StreamState::Error => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for ChunkStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }
}

/// Str is a wrapper to represent multiple datatypes as arrow
/// `DataType::Utf8`, i.e., a string.
struct Str<'a>(Cow<'a, str>);

impl<'a> Borrow<str> for Str<'a> {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl<'a> From<&'a str> for Str<'a> {
    fn from(value: &'a str) -> Self {
        Self(Cow::from(value))
    }
}

impl<'a> TryFrom<uuid::Uuid> for Str<'a> {
    type Error = std::str::Utf8Error;

    fn try_from(value: uuid::Uuid) -> Result<Self, Self::Error> {
        let mut buf = [0; 36];
        value.as_hyphenated().encode_lower(&mut buf);
        let s = std::str::from_utf8(&buf)?;
        Ok(Self(Cow::from(s.to_owned())))
    }
}

impl<'a> From<serde_json::Value> for Str<'a> {
    fn from(value: serde_json::Value) -> Self {
        Self(Cow::from(format!("{value}")))
    }
}

impl<'a> FromSql<'a> for Str<'a> {
    fn accepts(ty: &PostgresType) -> bool {
        type S<'a> = &'a str;
        S::accepts(ty)
            || ty == &PostgresType::UUID
            || ty == &PostgresType::JSON
            || ty == &PostgresType::JSONB
    }

    fn from_sql(
        ty: &PostgresType,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match ty.name() {
            "uuid" => Ok(uuid::Uuid::from_sql(ty, raw)?.try_into()?),
            "json" | "jsonb" => Ok(serde_json::Value::from_sql(ty, raw)?.into()),
            _ => {
                type S<'a> = &'a str;
                Ok(S::from_sql(ty, raw)?.into())
            }
        }
    }
}

/// Macro for generating the match arms when converting a binary row to a record
/// batch.
///
/// See the `DataType::Utf8` match arm in `binary_rows_to_record_batch` for an
/// idea of what this macro produces.
macro_rules! make_column {
    ($builder:ty, $rows:expr, $col_idx:expr) => {{
        let mut arr = <$builder>::with_capacity($rows.len());
        for row in $rows.iter() {
            arr.append_option(row.try_get($col_idx)?);
        }
        Arc::new(arr.finish())
    }};
}

/// Convert binary rows into a single record batch.
fn binary_rows_to_record_batch<E: Into<PostgresError>>(
    rows: Vec<Result<BinaryCopyOutRow, E>>,
    schema: ArrowSchemaRef,
) -> Result<RecordBatch> {
    use datafusion::arrow::array::{
        Array, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int16Builder, Int32Builder, Int64Builder, StringBuilder, Time64NanosecondBuilder,
        TimestampNanosecondBuilder,
    };

    let rows = rows
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.into())?;

    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields.len());
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
                    let val: Option<Str<'_>> = row.try_get(col_idx)?;
                    if let Some(v) = val {
                        let s: &str = v.borrow();
                        arr.append_value(s);
                    } else {
                        arr.append_null();
                    }
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
            dt @ DataType::Decimal128(_p, s) => {
                let mut arr =
                    Decimal128Builder::with_capacity(rows.len()).with_data_type(dt.clone());
                for row in rows.iter() {
                    let val: Option<rust_decimal::Decimal> = row.try_get(col_idx)?;
                    let val = match val {
                        Some(v) => {
                            let mut v =
                                decimal::Decimal128::new(v.mantissa(), v.scale().try_into()?)?;
                            v.rescale(*s);
                            Some(v.mantissa())
                        }
                        None => None,
                    };
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let mut arr = TimestampNanosecondBuilder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<NaiveDateTime> = row.try_get(col_idx)?;
                    let val = val.map(|v| v.timestamp_nanos());
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            dt @ DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
                let mut arr = TimestampNanosecondBuilder::with_capacity(rows.len())
                    .with_data_type(dt.clone());
                for row in rows.iter() {
                    let val: Option<DateTime<Utc>> = row.try_get(col_idx)?;
                    let val = val.map(|v| v.timestamp_nanos());
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let mut arr = Time64NanosecondBuilder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<NaiveTime> = row.try_get(col_idx)?;
                    let val = val.map(|v| {
                        let nanos = v.nanosecond() as i64;
                        let secs_since_midnight = v.num_seconds_from_midnight() as i64;
                        (secs_since_midnight * 1_000_000_000) + nanos
                    });
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Date32 => {
                let mut arr = Date32Builder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<NaiveDate> = row.try_get(col_idx)?;
                    let val = val.map(|v| v.signed_duration_since(epoch_date).num_days() as i32);
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            other => return Err(PostgresError::FailedBinaryCopy(other.clone())),
        };
        columns.push(col)
    }

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}

/// Create an arrow schema from a list of names and stringified postgres types.
fn try_create_arrow_schema(names: Vec<String>, types: &Vec<PostgresType>) -> Result<ArrowSchema> {
    let mut fields = Vec::with_capacity(names.len());
    let iter = names.into_iter().zip(types);

    for (name, typ) in iter {
        let arrow_typ = match typ {
            &PostgresType::BOOL => DataType::Boolean,
            &PostgresType::INT2 => DataType::Int16,
            &PostgresType::INT4 => DataType::Int32,
            &PostgresType::INT8 => DataType::Int64,
            &PostgresType::FLOAT4 => DataType::Float32,
            &PostgresType::FLOAT8 => DataType::Float64,
            &PostgresType::CHAR
            | &PostgresType::BPCHAR
            | &PostgresType::VARCHAR
            | &PostgresType::TEXT
            | &PostgresType::JSONB
            | &PostgresType::JSON
            | &PostgresType::UUID => DataType::Utf8,
            &PostgresType::BYTEA => DataType::Binary,
            // While postgres numerics are "unconstrained" by default, we need
            // to specify the precision and scale for the column. Setting these
            // same as bigquery.
            &PostgresType::NUMERIC => DataType::Decimal128(38, 9),
            &PostgresType::TIMESTAMP => DataType::Timestamp(TimeUnit::Nanosecond, None),
            &PostgresType::TIMESTAMPTZ => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            &PostgresType::TIME => DataType::Time64(TimeUnit::Nanosecond),
            &PostgresType::DATE => DataType::Date32,
            // TODO: Time with timezone and interval data types in postgres are
            // of 12 and 16 bytes respectively. This kind of size is not
            // supported by datafusion. Moreover, these datatypes are not
            // supported by the tokio-postgres library as well. What we need to
            // do is implement a data type that can support it and cast it to
            // datafusion `FixedSizeBinary` or something similar OR even cast
            // it to existing datafusion types (which would be reasonable but
            // might cause some data loss).
            other => {
                return Err(PostgresError::UnsupportedPostgresType(
                    other.name().to_owned(),
                ))
            }
        };

        // Assume all fields are nullable.
        let field = Field::new(name, arrow_typ, true);
        fields.push(field);
    }

    Ok(ArrowSchema::new(fields))
}

/// Convert filtering expressions to a predicate string usable with the
/// generated Postgres query.
fn exprs_to_predicate_string(exprs: &[Expr]) -> Result<String> {
    let mut ss = Vec::new();
    let mut buf = String::new();
    for expr in exprs {
        if write_expr(expr, &mut buf)? {
            ss.push(buf);
            buf = String::new();
        }
    }

    Ok(ss.join(" AND "))
}

/// Try to write the expression to the string, returning true if it was written.
fn write_expr(expr: &Expr, buf: &mut String) -> Result<bool> {
    match expr {
        Expr::Column(col) => {
            write!(buf, "{}", col)?;
        }
        Expr::Literal(val) => {
            util::encode_literal_to_text(util::Datasource::Postgres, buf, val)?;
        }
        Expr::IsNull(expr) => {
            if write_expr(expr, buf)? {
                write!(buf, " IS NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsNotNull(expr) => {
            if write_expr(expr, buf)? {
                write!(buf, " IS NOT NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsTrue(expr) => {
            if write_expr(expr, buf)? {
                write!(buf, " IS TRUE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsFalse(expr) => {
            if write_expr(expr, buf)? {
                write!(buf, " IS FALSE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::BinaryExpr(binary) => {
            if !write_expr(binary.left.as_ref(), buf)? {
                return Ok(false);
            }
            write!(buf, " {} ", binary.op)?;
            if !write_expr(binary.right.as_ref(), buf)? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{BinaryExpr, Operator};

    #[test]
    fn connection_string() {
        let conn_str = PostgresDbConnection::ConnectionString(
            "postgres://prod:password123@127.0.0.1:5432/glaredb".to_string(),
        )
        .connection_string();
        assert_eq!(
            &conn_str,
            "postgres://prod:password123@127.0.0.1:5432/glaredb"
        );

        let conn_str = PostgresDbConnection::Parameters {
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
            password: Some("password123".to_string()),
            database: "glaredb".to_string(),
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(
            &conn_str,
            "postgres://prod:password123@127.0.0.1:5432/glaredb"
        );

        // Missing password.
        let conn_str = PostgresDbConnection::Parameters {
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
            password: None,
            database: "glaredb".to_string(),
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "postgres://prod@127.0.0.1:5432/glaredb");

        // Missing port.
        let conn_str = PostgresDbConnection::Parameters {
            host: "127.0.0.1".to_string(),
            port: None,
            user: "prod".to_string(),
            password: Some("password123".to_string()),
            database: "glaredb".to_string(),
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "postgres://prod:password123@127.0.0.1/glaredb");
    }

    #[test]
    fn valid_expr_string() {
        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "a".to_string(),
                })),
                op: Operator::Lt,
                right: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "b".to_string(),
                })),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "c".to_string(),
                })),
                op: Operator::Lt,
                right: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "d".to_string(),
                })),
            }),
        ];

        let out = exprs_to_predicate_string(&exprs).unwrap();
        assert_eq!(out, "a < b AND c < d")
    }

    #[test]
    fn skip_unsupported_expr_string() {
        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "a".to_string(),
                })),
                op: Operator::Lt,
                right: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "b".to_string(),
                })),
            }),
            // Not currently supported for our expression writing.
            Expr::Sort(Sort {
                expr: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "a".to_string(),
                })),
                asc: true,
                nulls_first: true,
            }),
        ];

        let out = exprs_to_predicate_string(&exprs).unwrap();
        assert_eq!(out, "a < b")
    }
}
