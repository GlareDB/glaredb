pub mod errors;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::Statistics;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datasource_common::ssh::SshTunnelAccess;
use errors::{PostgresError, Result};
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{self, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_postgres::binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream};
use tokio_postgres::types::Type as PostgresType;
use tokio_postgres::{Config, CopyOutStream, NoTls};
use tracing::warn;

/// Information needed for accessing an external Postgres table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresTableAccess {
    /// The schema the table belongs to within postgres.
    pub schema: String,
    /// The table or view name inside of postgres.
    pub name: String,
    /// Database connection string.
    pub connection_string: String,
}

#[derive(Debug)]
pub struct PostgresAccessor {
    access: PostgresTableAccess,
    /// The Postgres client.
    client: tokio_postgres::Client,
    /// Handle for the underlying Postgres connection.
    ///
    /// Kept on struct to avoid dropping future.
    #[allow(dead_code)]
    conn_handle: JoinHandle<()>,
}

impl PostgresAccessor {
    /// Connect to a postgres instance.
    pub async fn connect(
        access: PostgresTableAccess,
        ssh_tunnel: Option<SshTunnelAccess>,
    ) -> Result<Self> {
        match ssh_tunnel {
            None => Self::connect_direct(access).await,
            Some(ssh_tunnel) => Self::connect_with_ssh_tunnel(access, ssh_tunnel).await,
        }
    }

    async fn connect_direct(access: PostgresTableAccess) -> Result<Self> {
        let (client, conn) = tokio_postgres::connect(&access.connection_string, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = conn.await {
                warn!(%e, "postgres connection errored");
            }
        });

        Ok(PostgresAccessor {
            access,
            client,
            conn_handle: handle,
        })
    }

    async fn connect_with_ssh_tunnel(
        access: PostgresTableAccess,
        ssh_tunnel: SshTunnelAccess,
    ) -> Result<Self> {
        let config: Config = access.connection_string.parse()?;

        // Accept only singular host and port for postgres access
        let postgres_host = match config.get_hosts() {
            [tokio_postgres::config::Host::Tcp(host)] => host.as_str(),
            hosts => return Err(PostgresError::IncorrectNumberOfHosts(hosts.to_vec())),
        };
        let postgres_port = match config.get_ports() {
            &[port] => port,
            &[] => 5432, // default postgres port
            ports => return Err(PostgresError::TooManyPorts(ports.to_vec())),
        };

        // Open ssh tunnel
        let (session, tunnel_addr) = ssh_tunnel
            .create_tunnel(postgres_host, postgres_port)
            .await?;

        let tcp_stream = TcpStream::connect(tunnel_addr).await?;
        let (client, conn) = config.connect_raw(tcp_stream, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = conn.await {
                warn!(%e, "postgres connection errored");
            }
            // If postgres connection is complete, close ssh tunnel
            if let Err(e) = session.close().await {
                warn!(%e, "closing ssh tunnel errored");
            }
        });

        Ok(PostgresAccessor {
            access,
            client,
            conn_handle: handle,
        })
    }

    pub async fn into_table_provider(
        self,
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

        // Get oid of table, and approx number of pages for the relation.
        let row = self
            .client
            .query_one(
                "
SELECT
    pg_class.oid,
    GREATEST(relpages, 1)
FROM pg_class INNER JOIN pg_namespace ON relnamespace = pg_namespace.oid
WHERE nspname=$1 AND relname=$2;
",
                &[&self.access.schema, &self.access.name],
            )
            .await?;
        let oid: u32 = row.try_get(0)?;
        // let approx_pages: i64 = row.try_get(1)?;

        // Get table schema.
        let rows = self
            .client
            .query(
                "
SELECT
    attname,
    pg_type.typname,
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
        let mut types: Vec<String> = Vec::with_capacity(rows.len());
        let mut type_oids: Vec<u32> = Vec::with_capacity(rows.len());
        for row in rows {
            names.push(row.try_get(0)?);
            types.push(row.try_get(1)?);
            type_oids.push(row.try_get(2)?);
        }

        let arrow_schema = try_create_arrow_schema(names, types)?;
        let pg_types = type_oids
            .iter()
            .map(|oid| PostgresType::from_oid(*oid))
            .collect::<Option<Vec<_>>>()
            .ok_or(PostgresError::UnknownPostgresOids(type_oids))?;

        Ok(PostgresTableProvider {
            predicate_pushdown,
            accessor: Arc::new(self),
            arrow_schema: Arc::new(arrow_schema),
            pg_types: Arc::new(pg_types),
        })
    }
}

pub struct PostgresTableProvider {
    predicate_pushdown: bool,
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
            projection_string,           // SELECT <str>
            self.accessor.access.schema, // FROM <schema>
            self.accessor.access.name,   // .<table>
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
            accessor: self.accessor.clone(),
            pg_types: projected_types,
            arrow_schema: projected_schema,
            opener,
        }))
    }
}

/// Copy data from the source Postgres table using the binary copy protocol.
struct BinaryCopyExec {
    predicate: String,
    accessor: Arc<PostgresAccessor>,
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

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BinaryCopyExec: schema={}, name={}, predicate={}",
            self.accessor.access.schema,
            self.accessor.access.name,
            if self.predicate.is_empty() {
                "None"
            } else {
                self.predicate.as_str()
            }
        )
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
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
    type Item = ArrowResult<RecordBatch>;

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
                        return Poll::Ready(Some(Err(ArrowError::ExternalError(Box::new(e)))));
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
                                return Poll::Ready(Some(Err(ArrowError::ExternalError(
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

/// Macro for generating the match arms when converting a binary row to a record
/// batch.
///
/// See the `DataType::Utf8` match arm in `binary_rows_to_record_batch` for an
/// idea of what this macro produces.
macro_rules! make_column {
    ($builder:ty, $rows:expr, $col_idx:expr) => {{
        let mut arr = <$builder>::with_capacity($rows.len());
        for row in $rows.iter() {
            arr.append_value(row.try_get($col_idx)?);
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
        Array, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, StringBuilder,
    };

    let rows = rows
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.into())?;

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
                    let val: &str = row.try_get(col_idx)?;
                    arr.append_value(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Binary => {
                // Assumes an average of 16 bytes per item.
                let mut arr = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows.iter() {
                    let val: &[u8] = row.try_get(col_idx)?;
                    arr.append_value(val);
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
// TODO: We could probably use postgres oids instead of strings for types.
fn try_create_arrow_schema(names: Vec<String>, types: Vec<String>) -> Result<ArrowSchema> {
    let mut fields = Vec::with_capacity(names.len());
    let iter = names.into_iter().zip(types.into_iter());

    for (name, typ) in iter {
        let arrow_typ = match typ.as_str() {
            "bool" => DataType::Boolean,
            "int2" => DataType::Int16,
            "int4" => DataType::Int32,
            "int8" => DataType::Int64,
            "float4" => DataType::Float32,
            "float8" => DataType::Float64,
            "char" | "bpchar" | "varchar" | "text" | "jsonb" | "json" => DataType::Utf8,
            "bytea" => DataType::Binary,
            other => return Err(PostgresError::UnsupportedPostgresType(other.to_string())),
        };

        // Assume all fields are nullable.
        let field = Field::new(&name, arrow_typ, true);
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
fn write_expr(expr: &Expr, s: &mut String) -> Result<bool> {
    match expr {
        Expr::Column(col) => {
            write!(s, "{}", col)?;
        }
        Expr::Literal(val) => match val {
            ScalarValue::Utf8(Some(utf8)) => write!(s, "'{}'", utf8)?,
            other => write!(s, "{}", other)?,
        },
        Expr::IsNull(expr) => {
            if write_expr(expr, s)? {
                write!(s, " IS NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsNotNull(expr) => {
            if write_expr(expr, s)? {
                write!(s, " IS NOT NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsTrue(expr) => {
            if write_expr(expr, s)? {
                write!(s, " IS TRUE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsFalse(expr) => {
            if write_expr(expr, s)? {
                write!(s, " IS FALSE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::BinaryExpr(binary) => {
            if !write_expr(binary.left.as_ref(), s)? {
                return Ok(false);
            }
            write!(s, " {} ", binary.op)?;
            if !write_expr(binary.right.as_ref(), s)? {
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
    use datafusion::logical_expr::{BinaryExpr, Operator};

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
            Expr::Sort {
                expr: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "a".to_string(),
                })),
                asc: true,
                nulls_first: true,
            },
        ];

        let out = exprs_to_predicate_string(&exprs).unwrap();
        assert_eq!(out, "a < b")
    }
}
