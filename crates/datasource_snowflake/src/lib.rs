pub mod errors;

use std::fmt::{self, Write};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayFormatType, Partitioning, RecordBatchStream, Statistics};
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    datasource::TableProvider,
    error::{DataFusionError, Result as DatafusionResult},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use datasource_common::errors::DatasourceCommonError;
use datasource_common::listing::{VirtualLister, VirtualTable};
use datasource_common::util;
use futures::{Stream, StreamExt};
use snowflake_connector::QueryResultChunk;
use snowflake_connector::{
    datatype::SnowflakeDataType, snowflake_to_arrow_datatype, Connection as SnowflakeConnection,
    QueryBindParameter,
};

use crate::errors::Result;

#[derive(Debug, Clone)]
pub struct SnowflakeDbConnection {
    pub account_name: String,
    pub login_name: String,
    pub password: String,
    pub database_name: String,
    pub warehouse: String,
    pub role_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SnowflakeTableAccess {
    pub schema_name: String,
    pub table_name: String,
}

pub struct SnowflakeAccessor {
    conn: SnowflakeConnection,
}

impl SnowflakeAccessor {
    pub async fn connect(conn_params: SnowflakeDbConnection) -> Result<Self> {
        let conn = Self::build_conn(conn_params).await?;
        Ok(Self { conn })
    }

    async fn build_conn(conn_params: SnowflakeDbConnection) -> Result<SnowflakeConnection> {
        let mut conn =
            SnowflakeConnection::builder(conn_params.account_name, conn_params.login_name)
                .password(conn_params.password)
                .database_name(conn_params.database_name)
                .warehouse(conn_params.warehouse);

        if let Some(role_name) = conn_params.role_name {
            conn = conn.role_name(role_name);
        }

        let conn = conn.build().await?;
        Ok(conn)
    }

    pub async fn validate_external_database(conn_params: SnowflakeDbConnection) -> Result<()> {
        let accessor = Self::connect(conn_params).await?;

        // Validate if the connection is Ok
        let query = "SELECT 1".to_string();
        let _res = accessor.conn.query(query, Vec::new()).await?;

        Ok(())
    }

    pub async fn validate_table_access(
        conn_params: SnowflakeDbConnection,
        table_access: &SnowflakeTableAccess,
    ) -> Result<ArrowSchema> {
        let accessor = Self::connect(conn_params).await?;

        // Validate if the connection is Ok
        let query = format!(
            "SELECT * FROM {}.{} WHERE FALSE",
            table_access.schema_name, table_access.table_name
        );
        let _res = accessor.conn.query(query, vec![]).await?;

        // Get table schema
        accessor.get_table_schema(table_access).await
    }

    async fn get_table_schema(&self, table_access: &SnowflakeTableAccess) -> Result<ArrowSchema> {
        // Snowflake stores data as upper-case. Maybe this won't be an issue
        // when we use bindings but for now, manually transform everything to
        // uppercase values.
        let table_schema = table_access.schema_name.to_uppercase();
        let table_name = table_access.table_name.to_uppercase();

        // TODO: There's time precision as well (nanos, micros...). Currently
        // assuming everything as default nanos. Test if setting precision
        // differently causes any problems, i.e., whether or not internally
        // the data is stored differently.
        let res = self
            .conn
            .query(
                "
SELECT
    column_name,
    data_type,
    numeric_precision,
    numeric_scale
FROM information_schema.columns
WHERE
    table_name=? AND
    table_schema=?
                "
                .to_string(),
                vec![
                    QueryBindParameter::new_text(table_name),
                    QueryBindParameter::new_text(table_schema),
                ],
            )
            .await?;

        let mut fields = Vec::new();
        for row in res.into_row_iter() {
            let row = row?;
            let col_name = match row.get_column_by_name("COLUMN_NAME").unwrap()? {
                // Convert the column name to lowercase since we every name
                // we match is case-insensitive.
                ScalarValue::Utf8(Some(v)) => v.to_lowercase(),
                _ => unreachable!(),
            };
            let data_type: SnowflakeDataType = match row.get_column_by_name("DATA_TYPE").unwrap()? {
                ScalarValue::Utf8(Some(v)) => v.parse()?,
                _ => unreachable!(),
            };
            let numeric_precision = match row.get_column_by_name("NUMERIC_PRECISION").unwrap()? {
                ScalarValue::Decimal128(v, _, 0) => v.map(|n| n as i64),
                _ => unreachable!(),
            };
            let numeric_scale = match row.get_column_by_name("NUMERIC_SCALE").unwrap()? {
                ScalarValue::Decimal128(v, _, 0) => v.map(|n| n as i64),
                _ => unreachable!(),
            };

            let arrow_type =
                snowflake_to_arrow_datatype(data_type, numeric_precision, numeric_scale);
            let field = Field::new(col_name, arrow_type, /* nullable = */ true);
            fields.push(field);
        }

        Ok(ArrowSchema::new(fields))
    }

    pub async fn into_table_provider(
        self,
        table_access: SnowflakeTableAccess,
        predicate_pushdown: bool,
    ) -> Result<SnowflakeTableProvider> {
        let arrow_schema = self.get_table_schema(&table_access).await?;

        Ok(SnowflakeTableProvider {
            predicate_pushdown,
            table_access,
            accessor: Arc::new(self),
            arrow_schema: Arc::new(arrow_schema),
        })
    }
}

#[async_trait]
impl VirtualLister for SnowflakeAccessor {
    async fn list_schemas(&self) -> Result<Vec<String>, DatasourceCommonError> {
        use DatasourceCommonError::ListingErrBoxed;

        let res = self
            .conn
            .query(
                "SELECT schema_name FROM information_schema.schemata".to_string(),
                Vec::new(),
            )
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        let mut schema_list = Vec::new();
        for row in res.into_row_iter() {
            let row = row.map_err(|e| ListingErrBoxed(Box::new(e)))?;

            let schema = match row
                .get_column(0)
                .unwrap()
                .map_err(|e| ListingErrBoxed(Box::new(e)))?
            {
                ScalarValue::Utf8(Some(v)) => v,
                _ => unreachable!(),
            };
            schema_list.push(schema);
        }

        Ok(schema_list)
    }

    async fn list_tables(
        &self,
        schema: Option<&str>,
    ) -> Result<Vec<VirtualTable>, DatasourceCommonError> {
        use DatasourceCommonError::ListingErrBoxed;

        const LIST_TABLES_QUERY: &str =
            "SELECT table_schema, table_name FROM information_schema.tables";
        let (query, bindings) = if let Some(schema) = schema {
            (
                format!("{LIST_TABLES_QUERY} WHERE table_schema = ?"),
                vec![QueryBindParameter::new_text(schema)],
            )
        } else {
            (LIST_TABLES_QUERY.to_owned(), Vec::new())
        };

        let res = self
            .conn
            .query(query, bindings)
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        let mut tables_list = Vec::new();
        for row in res.into_row_iter() {
            let row = row.map_err(|e| ListingErrBoxed(Box::new(e)))?;

            let schema = match row
                .get_column_by_name("TABLE_SCHEMA")
                .unwrap()
                .map_err(|e| ListingErrBoxed(Box::new(e)))?
            {
                ScalarValue::Utf8(Some(v)) => v,
                _ => unreachable!(),
            };

            let table = match row
                .get_column_by_name("TABLE_NAME")
                .unwrap()
                .map_err(|e| ListingErrBoxed(Box::new(e)))?
            {
                ScalarValue::Utf8(Some(v)) => v,
                _ => unreachable!(),
            };

            tables_list.push(VirtualTable { schema, table });
        }

        Ok(tables_list)
    }
}

pub struct SnowflakeTableProvider {
    predicate_pushdown: bool,
    table_access: SnowflakeTableAccess,
    accessor: Arc<SnowflakeAccessor>,
    arrow_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for SnowflakeTableProvider {
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
        // Projection
        let projection_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => Arc::clone(&self.arrow_schema),
        };

        let projection_string = projection_schema
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(",");

        let limit_string = match limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let predicate_string = if self.predicate_pushdown {
            exprs_to_predicate_string(filters)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            String::new()
        };

        let query = format!(
            "SELECT {} FROM {}.{} {} {} {}",
            projection_string,
            self.table_access.schema_name,
            self.table_access.table_name,
            if predicate_string.is_empty() {
                ""
            } else {
                "WHERE"
            },
            predicate_string,
            limit_string,
        );

        let result = self
            .accessor
            .conn
            .query(query, Vec::new())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(SnowflakeExec {
            predicate: predicate_string,
            arrow_schema: projection_schema,
            result: Mutex::new(Some(result)),
        }))
    }
}

struct SnowflakeExec {
    predicate: String,
    arrow_schema: ArrowSchemaRef,
    // TODO: Once we have the async queries implemented on the connector side,
    // we can fetch the different streams and execute each one in a seperate
    // partition just like we do in BigQuery. Currently using a simple mutex
    // and taking the value from the option.
    result: Mutex<Option<QueryResultChunk>>,
}

impl ExecutionPlan for SnowflakeExec {
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
            "cannot replace children for Snowflake exec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> DatafusionResult<datafusion::physical_plan::SendableRecordBatchStream> {
        let result = {
            let mut guard = self.result.lock().unwrap();
            guard.take().expect("iter shouldn't be None")
        };
        Ok(Box::pin(ChunkStream::new(self.schema(), result)))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SnowflakeExec: predicate={}",
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

impl fmt::Debug for SnowflakeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeExec")
            .field("predicate", &self.predicate)
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

type PinnedStream = Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send + Sync>>;

struct ChunkStream {
    schema: ArrowSchemaRef,
    inner: PinnedStream,
}

impl ChunkStream {
    fn new(schema: ArrowSchemaRef, result: QueryResultChunk) -> Self {
        let stream = async_stream::stream! {
            for batch in result.into_iter() {
                let batch = batch?;
                yield Ok(batch);
            }
        };
        let inner = Box::pin(stream);
        Self { schema, inner }
    }
}

impl Stream for ChunkStream {
    type Item = DatafusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ChunkStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

/// Convert filtering expressions to a predicate string usable with Snowflake's
/// row restriction.
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
            util::encode_literal_to_text(util::Datasource::Snowflake, buf, val)?;
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
