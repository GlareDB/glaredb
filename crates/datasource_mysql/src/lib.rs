pub mod errors;

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use datasource_common::ssh::SshTunnelAccess;
use errors::{MysqlError, Result};
use mysql_async::consts::ColumnFlags;
use mysql_async::consts::ColumnType;
use mysql_async::{prelude::*, Column as MysqlColumn, Row as MysqlRow};
use mysql_async::{Conn, IsolationLevel, Opts, TxOpts};
use serde::{Deserialize, Serialize};

/// Information needed for accessing an external Mysql table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlTableAccess {
    /// The database or schema the table belongs to within mysql.
    pub schema: String,
    /// The table or view name inside of mysql.
    pub name: String,
    /// Database connection string.
    pub connection_string: String,
}

#[derive(Debug)]
pub struct MysqlAccessor {
    access: MysqlTableAccess,
    conn: mysql_async::Conn,
}

#[allow(unused)] //TODO Remove
impl MysqlAccessor {
    /// Connect to a mysql instance.
    pub async fn connect(
        access: MysqlTableAccess,
        ssh_tunnel: Option<SshTunnelAccess>,
    ) -> Result<Self> {
        match ssh_tunnel {
            None => Self::connect_direct(access).await,
            Some(ssh_tunnel) => Self::connect_with_ssh_tunnel(access, ssh_tunnel).await,
        }
    }

    async fn connect_direct(access: MysqlTableAccess) -> Result<Self> {
        let database_url = &access.connection_string;

        let opts = Opts::from_url(database_url)?;
        let mut conn = Conn::new(opts).await?;

        tracing::trace!(opts=?conn.opts(), "Successfully connected to mysql db");

        let res = conn.query::<MysqlRow, _>("SELECT * FROM TEST.T1").await?;

        tracing::trace!(res=?res, "Query T1");

        Ok(MysqlAccessor { access, conn })
    }

    async fn connect_with_ssh_tunnel(
        access: MysqlTableAccess,
        ssh_tunnel: SshTunnelAccess,
    ) -> Result<Self> {
        return Err(MysqlError::Unimplemented);
        // Open ssh tunnel
        // let (session, tunnel_addr) = ssh_tunnel.create_tunnel(mysql_host, mysql_port).await?;
        // let tcp_stream = TcpStream::connect(tunnel_addr).await?;
        // Ok(MysqlAccessor { access })
    }

    pub async fn into_table_provider(
        mut self,
        predicate_pushdown: bool,
    ) -> Result<MysqlTableProvider> {
        let mut tx_opts = TxOpts::new();
        tx_opts
            .with_isolation_level(IsolationLevel::RepeatableRead)
            .with_readonly(true);

        let mut tx = self.conn.start_transaction(tx_opts).await?;

        let cols = tx
            .query_iter(format!(
                "SELECT * FROM {}.{} where false",
                self.access.schema, self.access.name
            ))
            .await?;
        let cols = cols.columns_ref();

        tracing::warn!(?cols, len = cols.len());
        // Genrate arrow schema from table schema - should be done now
        let arrow_schema = try_create_arrow_schema(cols)?;

        tracing::warn!(?arrow_schema, len = arrow_schema.fields().len());

        return Err(MysqlError::Unimplemented);
        // Genrate mysql types info a from table schema
        let mysql_types = todo!();

        //TODO tx.commit()

        Ok(MysqlTableProvider {
            predicate_pushdown,
            accessor: Arc::new(self),
            arrow_schema: Arc::new(arrow_schema),
            mysql_types: Arc::new(mysql_types),
        })
    }
}

//TODO update to library copy
#[derive(Debug, Clone)]
struct MysqlType {}

#[allow(unused)] // TODO remove
pub struct MysqlTableProvider {
    predicate_pushdown: bool,
    accessor: Arc<MysqlAccessor>,
    arrow_schema: ArrowSchemaRef,
    mysql_types: Arc<Vec<MysqlType>>, //possibley done at scan time
}

#[async_trait]
#[allow(unused)] // TODO remove
impl TableProvider for MysqlTableProvider {
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
        return Err(DataFusionError::External(Box::new(
            MysqlError::Unimplemented,
        )));

        // Project the schema.
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        // Project the mysql types so that it matches the ouput schema.
        let projected_types = match projection {
            Some(projection) => Arc::new(
                projection
                    .iter()
                    .map(|i| self.mysql_types[*i].clone())
                    .collect::<Vec<_>>(),
            ),
            None => self.mysql_types.clone(),
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

        todo!()
    }
}

#[allow(unused)] // TODO remove
fn exprs_to_predicate_string(filters: &[Expr]) -> Result<String> {
    return Err(MysqlError::Unimplemented);
}

#[derive(Debug)]
#[allow(unused)] // TODO remove
struct MysqlExec {
    predicate: String,
    accessor: Arc<MysqlAccessor>,
    mysql_types: Arc<Vec<MysqlType>>,
    arrow_schema: ArrowSchemaRef,
}

impl ExecutionPlan for MysqlExec {
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
            "cannot replace children for MysqlExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        return Err(DataFusionError::External(Box::new(
            MysqlError::Unimplemented,
        )));
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MysqlExec: schema={}, name={}, predicate={}",
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

/// Create an arrow schema from a list of names and stringified postgres types.
// TODO: We could probably use postgres oids instead of strings for types.
fn try_create_arrow_schema(cols: &[MysqlColumn]) -> Result<ArrowSchema> {
    let mut fields = Vec::with_capacity(cols.len());

    let iter = cols
        .into_iter()
        .map(|c| (c, c.name_str(), c.column_type(), c.flags()));

    for (col, name, typ, flags) in iter {
        tracing::warn!(?col);
        tracing::warn!(%name, ?typ, ?flags);
        use ColumnType::*;

        // Column definiton flags can be found here: https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
        let unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
        let blob = flags.contains(ColumnFlags::BLOB_FLAG);
        let binary = flags.contains(ColumnFlags::BINARY_FLAG);
        let timestamp = flags.contains(ColumnFlags::TIMESTAMP_FLAG);
        let timestamp = flags.contains(ColumnFlags::TIMESTAMP_FLAG);
        let arrow_typ = match typ {
            // TODO: more types
            // "float4" => DataType::Float32,
            // "float8" => DataType::Float64,
            // "char" | "bpchar" | "varchar" | "text" | "jsonb" | "json" => DataType::Utf8,
            // "bytea" => DataType::Binary,
            MYSQL_TYPE_DECIMAL => todo!(),
            // TINYINT
            MYSQL_TYPE_TINY if unsigned => DataType::UInt8,
            MYSQL_TYPE_TINY => DataType::Int8,
            // SMALLINT
            MYSQL_TYPE_SHORT if unsigned => DataType::UInt16,
            MYSQL_TYPE_SHORT => DataType::Int16,
            // INT
            MYSQL_TYPE_LONG if unsigned => DataType::UInt32,
            MYSQL_TYPE_LONG => DataType::Int32,
            MYSQL_TYPE_FLOAT => DataType::Float32,
            MYSQL_TYPE_DOUBLE => DataType::Float64,
            MYSQL_TYPE_NULL => todo!(),
            MYSQL_TYPE_TIMESTAMP => todo!(),
            // BIGINT
            MYSQL_TYPE_LONGLONG if unsigned => DataType::UInt64,
            MYSQL_TYPE_LONGLONG => DataType::Int64,
            // MEDIUMINT
            MYSQL_TYPE_INT24 if unsigned => DataType::UInt32,
            MYSQL_TYPE_INT24 => DataType::Int32,
            MYSQL_TYPE_DATE => todo!(),
            MYSQL_TYPE_TIME => todo!(),
            MYSQL_TYPE_DATETIME => todo!(),
            MYSQL_TYPE_YEAR => todo!(),
            MYSQL_TYPE_NEWDATE => todo!(),
            MYSQL_TYPE_VARCHAR => todo!(),
            MYSQL_TYPE_BIT => todo!(),
            MYSQL_TYPE_TIMESTAMP2 => todo!(),
            MYSQL_TYPE_DATETIME2 => todo!(),
            MYSQL_TYPE_TIME2 => todo!(),
            MYSQL_TYPE_TYPED_ARRAY => todo!(),
            MYSQL_TYPE_UNKNOWN => todo!(),
            MYSQL_TYPE_JSON => todo!(),
            MYSQL_TYPE_NEWDECIMAL => DataType::Decimal128(
                col.decimals(),
                i8::try_from(col.column_length() - col.decimals() as u32)?,
            ),
            MYSQL_TYPE_ENUM => todo!(),
            MYSQL_TYPE_SET => todo!(),
            MYSQL_TYPE_TINY_BLOB => todo!(),
            MYSQL_TYPE_MEDIUM_BLOB => todo!(),
            MYSQL_TYPE_LONG_BLOB => todo!(),
            MYSQL_TYPE_BLOB => todo!(),
            MYSQL_TYPE_VAR_STRING => DataType::Utf8,
            MYSQL_TYPE_STRING => todo!(),
            MYSQL_TYPE_GEOMETRY => todo!(),
            other @ _ => return Err(MysqlError::UnsupportedMysqlType((other as u8).to_string())),
        };

        let nullable = flags.contains(ColumnFlags::NOT_NULL_FLAG);
        let field = Field::new(name, arrow_typ, nullable);
        fields.push(field);
    }

    Ok(ArrowSchema::new(fields))
}
