pub mod errors;

mod query_exec;

use std::any::Any;
use std::borrow::{Borrow, Cow};
use std::fmt::{self, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use chrono::naive::{NaiveDateTime, NaiveTime};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use datafusion::arrow::array::{
    Array,
    BinaryBuilder,
    BooleanBuilder,
    Date32Builder,
    Decimal128Builder,
    Float32Builder,
    Float64Builder,
    Int16Builder,
    Int32Builder,
    Int64Builder,
    StringBuilder,
    Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{
    DataType,
    Field,
    Fields,
    Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
    TimeUnit,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    execute_stream,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    RecordBatchStream,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion::scalar::ScalarValue;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use errors::{PostgresError, Result};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{ready, FutureExt, Stream, StreamExt};
use protogen::metastore::types::options::TunnelOptions;
use protogen::{FromOptionalField, ProtoConvError};
use rustls::ClientConfig;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_postgres::binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream};
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::types::{FromSql, Type as PostgresType};
use tokio_postgres::{Client, Config, Connection, CopyOutStream, NoTls, Socket};
use tracing::{debug, warn};

use self::query_exec::PostgresInsertExec;
use crate::common::ssh::key::SshKey;
use crate::common::ssh::session::{SshTunnelAccess, SshTunnelSession};
use crate::common::util::{self, create_count_record_batch};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Information needed for create a postgres client.
#[derive(Debug, Clone)]
pub struct PostgresAccess {
    /// Connection string to the postgres instance.
    pub conn_str: PostgresDbConnection,
    /// Tunnel to use to access instance.
    pub tunnel: Option<TunnelOptions>,
}

impl PostgresAccess {
    pub fn new_from_conn_str(
        conn_str: impl Into<String>,
        tunnel: Option<TunnelOptions>,
    ) -> PostgresAccess {
        PostgresAccess {
            conn_str: PostgresDbConnection::ConnectionString(conn_str.into()),
            tunnel,
        }
    }

    /// Connect to an instance using these connection details.
    pub async fn connect(&self) -> Result<PostgresAccessState> {
        let state =
            PostgresAccessState::connect(&self.conn_str.connection_string(), self.tunnel.clone())
                .await?;
        Ok(state)
    }

    /// Validates access to the postgres instance.
    pub async fn validate_access(&self) -> Result<()> {
        let state = self.connect().await?;
        state.client.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    /// Validates access to a single table.
    pub async fn validate_table_access(&self, schema: &str, table: &str) -> Result<()> {
        let state = self.connect().await?;
        let query = format!("SELECT * FROM {}.{} where false", schema, table);
        state.client.execute(query.as_str(), &[]).await?;
        Ok(())
    }
}

impl TryFrom<protogen::sqlexec::common::PostgresAccess> for PostgresAccess {
    type Error = ProtoConvError;
    fn try_from(
        value: protogen::sqlexec::common::PostgresAccess,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new_from_conn_str(
            value.conn_str,
            value.tunnel.map(|t| t.try_into()).transpose()?,
        ))
    }
}

impl From<PostgresAccess> for protogen::sqlexec::common::PostgresAccess {
    fn from(value: PostgresAccess) -> Self {
        Self {
            conn_str: value.conn_str.connection_string(),
            tunnel: value.tunnel.map(|t| t.into()),
        }
    }
}

#[derive(Debug)]
pub struct PostgresAccessState {
    /// The Postgres client.
    client: tokio_postgres::Client,
    /// Handle for the underlying Postgres connection.
    /// Also contains the `Session` for the underlying ssh tunnel
    ///
    /// Kept on struct to avoid dropping the postgres connection future and ssh tunnel.
    #[allow(dead_code)]
    conn_handle: JoinHandle<()>,
}

impl PostgresAccessState {
    /// Connect to a postgres instance.
    async fn connect(connection_string: &str, tunnel: Option<TunnelOptions>) -> Result<Self> {
        let (client, conn_handle) = Self::connect_internal(connection_string, tunnel).await?;

        Ok(PostgresAccessState {
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

        let mut root_store = rustls::RootCertStore::empty();
        root_store
            .roots
            .extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|r| r.to_owned()));


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
                let mut root_store = rustls::RootCertStore::empty();
                root_store
                    .roots
                    .extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|r| r.to_owned()));


                match tokio_postgres::connect(
                    connection_string,
                    tokio_postgres_rustls::MakeRustlsConnect::new(
                        ClientConfig::builder()
                            .with_root_certificates(root_store)
                            .with_no_client_auth(),
                    ),
                )
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
                let mut root_store = rustls::RootCertStore::empty();
                root_store
                    .roots
                    .extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|r| r.to_owned()));

                let (client, conn) = tokio_postgres::connect(
                    connection_string,
                    tokio_postgres_rustls::MakeRustlsConnect::new(
                        ClientConfig::builder()
                            .with_root_certificates(root_store)
                            .with_no_client_auth(),
                    ),
                )
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

        let mut root_store = rustls::RootCertStore::empty();
        root_store
            .roots
            .extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|r| r.to_owned()));


        let tcp_stream = TcpStream::connect(tunnel_addr).await?;
        let mut tls_conf = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        tls_conf.enable_sni = false;

        // Configure tls depending on ssl mode.
        //
        // - Disable => no tls
        // - Prefer => try tls, fallback to no tls on connection error
        // - Require => try tls, no fallback
        // - Any other => return unsupported
        Ok(match config.get_ssl_mode() {
            SslMode::Disable => {
                let (client, conn) = config.connect_raw(tcp_stream, NoTls).await?;
                let handle = spawn_conn(conn, session);
                (client, handle)
            }
            SslMode::Prefer => {
                // Rust doesn't feel like type inferring this for us.
                match <tokio_postgres_rustls::MakeRustlsConnect as MakeTlsConnect<
                    TcpStream,
                >>::make_tls_connect(
                    &mut tokio_postgres_rustls::MakeRustlsConnect::new(tls_conf),
                    // TODO: Which host do we want to specify? Since this is being tunneled
                    // over ssh, I don't know if SNI actually matters.
                    "",
                ) {
		    Ok(tls_connect) => match config.connect_raw(tcp_stream, tls_connect).await {
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
                    },
		    Err(e) => {
                        // The tokio postgres lib does discriminate between tls
                        // and other errors, but that's not made public.
                        debug!(%e, "cert for pg conn falling back to no tls");
                        // Reconnect to get a fresh stream.
                        let tcp_stream = TcpStream::connect(tunnel_addr).await?;
                        let (client, conn) = config.connect_raw(tcp_stream, NoTls).await?;
                        let handle = spawn_conn(conn, session);
			(client, handle)
		    }
		}
            }
            SslMode::Require => {
                // Rust doesn't feel like type inferring this for us.
                let tls_connect = <tokio_postgres_rustls::MakeRustlsConnect as MakeTlsConnect<
                    TcpStream,
                >>::make_tls_connect(
                    &mut tokio_postgres_rustls::MakeRustlsConnect::new(tls_conf),
                    // TODO: Which host do we want to specify? Since this is being tunneled
                    // over ssh, I don't know if SNI actually matters.
                    "",
                )?;

                let (client, conn) = config.connect_raw(tcp_stream, tls_connect).await?;
                let handle = spawn_conn(conn, session);
                (client, handle)
            }
            other => return Err(PostgresError::UnsupportSslMode(other)),
        })
    }

    async fn get_table_schema(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<(ArrowSchema, Vec<PostgresType>)> {
        // TODO: Get schema using `information_schema.columns`. You can get
        // `numeric_precision` and `numeric_scale` as well from there so you
        // don't have to guess.

        // Get oid of table, and approx number of pages for the relation.
        let mut rows = self
            .client
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
        let rows = self
            .client
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

        let mut unknown_type_oids: Vec<u32> = Vec::new();

        type_oids.iter().for_each(|oid| {
            if PostgresType::from_oid(*oid).is_none() {
                unknown_type_oids.push(*oid);
            }
        });

        if !unknown_type_oids.is_empty() {
            return Err(PostgresError::UnknownPostgresOids(unknown_type_oids));
        }

        //will Proceed here ONLY if types are known for all oids else returns error with unknown_type_oids

        let pg_types = type_oids
            .iter()
            .map(|oid| PostgresType::from_oid(*oid))
            .collect::<Option<Vec<_>>>()
            .ok_or(PostgresError::UnknownPostgresOids(type_oids))?;

        let arrow_schema = try_create_arrow_schema(names, &pg_types)?;
        Ok((arrow_schema, pg_types))
    }
}

#[async_trait]
impl VirtualLister for PostgresAccessState {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        use ExtensionError::ListingErrBoxed;

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

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        use ExtensionError::ListingErrBoxed;

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

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        use ExtensionError::ListingErrBoxed;

        let (schema, _) = self
            .get_table_schema(schema, table)
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        Ok(schema.fields)
    }
}

pub struct PostgresTableProviderConfig {
    pub access: PostgresAccess,
    pub schema: String,
    pub table: String,
}

impl TryFrom<protogen::sqlexec::table_provider::PostgresTableProviderConfig>
    for PostgresTableProviderConfig
{
    type Error = ProtoConvError;
    fn try_from(
        value: protogen::sqlexec::table_provider::PostgresTableProviderConfig,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            access: value.access.required("postgres access")?,
            schema: value.schema,
            table: value.table,
        })
    }
}

impl From<PostgresTableProviderConfig>
    for protogen::sqlexec::table_provider::PostgresTableProviderConfig
{
    fn from(value: PostgresTableProviderConfig) -> Self {
        Self {
            access: Some(value.access.into()),
            schema: value.schema,
            table: value.table,
        }
    }
}

/// Table provider for a single postgres table.
pub struct PostgresTableProvider {
    /// Schema name of table we're accessing.
    schema: String,
    /// Table we're accessing.
    table: String,
    state: Arc<PostgresAccessState>,
    arrow_schema: ArrowSchemaRef,
    pg_types: Arc<Vec<PostgresType>>,
}

impl PostgresTableProvider {
    /// Try to create a new postgres table provider.
    ///
    /// If postgres access state (client) isn't provided, a new one will be
    /// created.
    pub async fn try_new(conf: PostgresTableProviderConfig) -> Result<Self> {
        let PostgresTableProviderConfig {
            access,
            schema,
            table,
        } = conf;

        let state = Arc::new(access.connect().await?);
        let (arrow_schema, pg_types) = state.get_table_schema(&schema, &table).await?;

        Ok(PostgresTableProvider {
            schema,
            table,
            state,
            arrow_schema: Arc::new(arrow_schema),
            pg_types: Arc::new(pg_types),
        })
    }
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

        let limit_string = match limit {
            Some(limit) => format!("LIMIT {}", limit),
            None => String::new(),
        };

        // Build WHERE clause if predicate pushdown enabled.
        //
        // TODO: This may produce an invalid clause. We'll likely only want to
        // convert some predicates.
        let predicate_string = {
            exprs_to_predicate_string(filters)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        // Build copy query.
        let query = format!(
            "COPY (SELECT {} FROM {}.{} {} {} {}) TO STDOUT (FORMAT binary)",
            projection_string, // SELECT <str>
            self.schema,       // FROM <schema>
            self.table,        // .<table>
            // [WHERE]
            if predicate_string.is_empty() {
                ""
            } else {
                "WHERE "
            },
            predicate_string.as_str(), // <where-predicate>
            limit_string,              // [LIMIT ..]
        );

        let exec = PostgresBinaryCopyExec::try_new(BinaryCopyConfig::State {
            copy_query: query,
            state: self.state.clone(),
            pg_types: projected_types,
            arrow_schema: projected_schema,
        })
        .await
        .unwrap(); // Should never error.

        Ok(Arc::new(exec))
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let mut values = String::new();

        let mut input = execute_stream(input, state.task_ctx())?;
        while let Some(batch) = input.next().await {
            let batch = batch?;

            let mut row_sep = "";
            for row_idx in 0..batch.num_rows() {
                write!(&mut values, "{row_sep}(")?;
                row_sep = ",";

                let mut col_sep = "";
                for col in batch.columns() {
                    write!(&mut values, "{col_sep}")?;
                    col_sep = ",";

                    let val = ScalarValue::try_from_array(col.as_ref(), row_idx)?;
                    util::encode_literal_to_text(util::Datasource::Postgres, &mut values, &val)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                }

                write!(&mut values, ")")?;
            }
        }

        if values.is_empty() {
            let batch = create_count_record_batch(0);
            let schema = batch.schema();
            return Ok(Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?));
        }

        let query = format!(
            "INSERT INTO {}.{} VALUES {}",
            self.schema, self.table, values
        );

        debug!(%query, "inserting into postgres datasource");

        let exec = PostgresInsertExec::new(query, self.state.clone());
        Ok(Arc::new(exec))
    }
}

#[derive(Debug, Clone)]
pub enum BinaryCopyConfig {
    /// Serializable config.
    AccessOnly {
        access: Box<PostgresAccess>,
        schema: String,
        table: String,
        copy_query: String,
    },
    /// Not serializable config.
    State {
        copy_query: String,
        state: Arc<PostgresAccessState>,
        pg_types: Arc<Vec<PostgresType>>,
        arrow_schema: ArrowSchemaRef,
    },
}

impl TryFrom<protogen::sqlexec::physical_plan::PostgresBinaryCopyConfig> for BinaryCopyConfig {
    type Error = ProtoConvError;
    fn try_from(
        value: protogen::sqlexec::physical_plan::PostgresBinaryCopyConfig,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::AccessOnly {
            access: Box::new(value.access.required("postgres access")?),
            schema: value.schema,
            table: value.table,
            copy_query: value.copy_query,
        })
    }
}

impl TryFrom<BinaryCopyConfig> for protogen::sqlexec::physical_plan::PostgresBinaryCopyConfig {
    type Error = ProtoConvError;
    fn try_from(value: BinaryCopyConfig) -> std::result::Result<Self, Self::Error> {
        match value {
            BinaryCopyConfig::State { .. } => Err(ProtoConvError::UnsupportedSerialization(
                "cannot serialize a stateful binary copy config",
            )),
            BinaryCopyConfig::AccessOnly {
                access,
                schema,
                table,
                copy_query,
            } => Ok(Self {
                access: Some((*access).into()),
                schema,
                table,
                copy_query,
            }),
        }
    }
}

/// Copy data from the source Postgres table using the binary copy protocol.
pub struct PostgresBinaryCopyExec {
    pg_types: Arc<Vec<PostgresType>>,
    arrow_schema: ArrowSchemaRef,
    opener: StreamOpener,
    metrics: ExecutionPlanMetricsSet,
}

impl PostgresBinaryCopyExec {
    /// Try to create a new binary copy exec using the provided config.
    pub async fn try_new(conf: BinaryCopyConfig) -> Result<Self> {
        match conf {
            BinaryCopyConfig::AccessOnly {
                access,
                schema,
                table,
                copy_query,
            } => {
                let state = Arc::new(access.connect().await?);
                let (arrow_schema, pg_types) = state.get_table_schema(&schema, &table).await?;
                let opener = StreamOpener { copy_query, state };
                Ok(PostgresBinaryCopyExec {
                    pg_types: Arc::new(pg_types),
                    arrow_schema: Arc::new(arrow_schema),
                    opener,
                    metrics: ExecutionPlanMetricsSet::new(),
                })
            }
            BinaryCopyConfig::State {
                copy_query,
                state,
                pg_types,
                arrow_schema,
            } => {
                let opener = StreamOpener { copy_query, state };
                Ok(PostgresBinaryCopyExec {
                    pg_types,
                    arrow_schema,
                    opener,
                    metrics: ExecutionPlanMetricsSet::new(),
                })
            }
        }
    }
}

impl ExecutionPlan for PostgresBinaryCopyExec {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for PostgresBinaryCopyExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let stream = ChunkStream {
            state: StreamState::Idle,
            types: self.pg_types.clone(),
            opener: self.opener.clone(),
            arrow_schema: self.arrow_schema.clone(),
        };
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

impl DisplayAs for PostgresBinaryCopyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PostgresBinaryCopyExec",)
    }
}

impl fmt::Debug for PostgresBinaryCopyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresBinaryCopyExec")
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
    state: Arc<PostgresAccessState>,
}

impl StreamOpener {
    /// Build a future that returns the copy stream.
    fn open(&self) -> BoxFuture<'static, Result<CopyOutStream, tokio_postgres::Error>> {
        let query = self.copy_query.clone();
        let accessor = self.state.clone();
        Box::pin(async move { accessor.client.copy_out(&query).await })
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
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return Ok(RecordBatch::try_new_with_options(schema, Vec::new(), &options).unwrap());
    }

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
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let mut arr = TimestampMicrosecondBuilder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<NaiveDateTime> = row.try_get(col_idx)?;
                    let val = val.map(|v| v.and_utc().timestamp_micros());
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                let mut arr = TimestampMicrosecondBuilder::with_capacity(rows.len())
                    .with_timezone(tz.clone());
                for row in rows.iter() {
                    let val: Option<DateTime<Utc>> = row.try_get(col_idx)?;
                    let val = val.map(|v| v.timestamp_micros());
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let mut arr = Time64MicrosecondBuilder::with_capacity(rows.len());
                for row in rows.iter() {
                    let val: Option<NaiveTime> = row.try_get(col_idx)?;
                    let val = val.map(|v| {
                        let sub_micros = (v.nanosecond() / 1_000) as i64;
                        let secs_since_midnight = v.num_seconds_from_midnight() as i64;
                        (secs_since_midnight * 1_000_000) + sub_micros
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
            &PostgresType::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
            &PostgresType::TIMESTAMPTZ => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            &PostgresType::TIME => DataType::Time64(TimeUnit::Microsecond),
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
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{BinaryExpr, Operator};

    use super::*;

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
