pub mod errors;

mod convert;
mod wrapper;

use std::any::Any;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_sqlite::rusqlite::types::{Type, Value};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{BinaryExpr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    RecordBatchStream,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::Expr;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use futures::{Stream, StreamExt};

use self::convert::Converter;
use self::errors::Result;
use self::wrapper::SqliteAsyncClient;
use crate::common::util;

type DataFusionResult<T> = Result<T, DataFusionError>;

#[derive(Debug, Clone)]
pub struct SqliteAccess {
    pub db: PathBuf,
}

impl SqliteAccess {
    pub async fn connect(&self) -> Result<SqliteAccessState> {
        let client = SqliteAsyncClient::new(self.db.to_path_buf()).await?;
        Ok(SqliteAccessState { client })
    }

    pub async fn validate_access(&self) -> Result<()> {
        let state = self.connect().await?;
        let _ = state.client.query_all("SELECT 1").await?;
        Ok(())
    }

    pub async fn validate_table_access(&self, table: &str) -> Result<()> {
        let state = self.connect().await?;
        let query = format!("SELECT * FROM {table} WHERE FALSE");
        let _ = state.client.query_all(query).await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SqliteAccessState {
    client: SqliteAsyncClient,
}

impl SqliteAccessState {
    async fn get_table_schema(&self, table: &str) -> Result<Schema> {
        const NUM_ROWS_TO_INFER_SCHEMA: usize = 10;

        let batch = self
            .client
            .query_all(format!(
                "SELECT * FROM {table} LIMIT {NUM_ROWS_TO_INFER_SCHEMA}"
            ))
            .await?;

        fn merge_data_types(rows: &[Type]) -> DataType {
            if rows.iter().any(|d| matches!(d, Type::Blob)) {
                return DataType::Binary;
            }

            if rows.iter().any(|d| matches!(d, Type::Text)) {
                return DataType::Utf8;
            }

            if rows.iter().any(|d| matches!(d, Type::Real)) {
                return DataType::Float64;
            }

            if rows.iter().any(|d| matches!(d, Type::Integer)) {
                return DataType::Int64;
            }

            // We got all null values, so keep the type "text" so we can convert
            // everything into it.
            DataType::Utf8
        }

        // Merge datatypes to get a valid schema
        let fields = batch
            .cols
            .into_iter()
            .enumerate()
            .map(|(col_id, col)| {
                let dt = if let Some(dt) = col.decl_type {
                    dt
                } else {
                    let data_types = batch
                        .data
                        .iter()
                        .map(|row| row[col_id].data_type())
                        .collect::<Vec<_>>();
                    merge_data_types(&data_types)
                };
                Field::new(col.name, dt, /* nullable = */ true)
            })
            .collect::<Vec<_>>();

        Ok(Schema::new(fields))
    }
}

#[async_trait]
impl VirtualLister for SqliteAccessState {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        // Sqlite doesn't have any "schemas". All tables belong to the same
        // schema. Naming it "default" here.
        Ok(vec!["default".to_string()])
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        if schema == "default" {
            let batch = self
                .client
                .query_all(
                    "SELECT name FROM sqlite_schema
                    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                )
                .await
                .map_err(ExtensionError::access)?;

            Ok(batch
                .data
                .into_iter()
                .filter_map(|mut row| {
                    if let Some(Value::Text(table_name)) = row.pop() {
                        Some(table_name)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>())
        } else {
            Err(ExtensionError::MissingObject {
                obj_typ: "schema",
                name: schema.to_owned(),
            })
        }
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        if schema == "default" {
            let table_schema = self
                .get_table_schema(table)
                .await
                .map_err(ExtensionError::access)?;
            Ok(table_schema.fields)
        } else {
            Err(ExtensionError::MissingObject {
                obj_typ: "schema",
                name: schema.to_owned(),
            })
        }
    }
}

pub struct SqliteTableProvider {
    state: SqliteAccessState,
    table: String,
    schema: SchemaRef,
}

impl SqliteTableProvider {
    pub async fn try_new(state: SqliteAccessState, table: impl Into<String>) -> Result<Self> {
        let table = table.into();
        let schema = state.get_table_schema(&table).await?;
        Ok(Self {
            state,
            table,
            schema: Arc::new(schema),
        })
    }
}

#[async_trait]
impl TableProvider for SqliteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Project the schema.
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

        let limit_string = match limit {
            Some(limit) => format!("LIMIT {}", limit),
            None => String::new(),
        };

        // Build WHERE clause if predicate pushdown enabled.
        //
        // TODO: This may produce an invalid clause. We'll likely only want to
        // convert some predicates.
        let predicate_string = {
            exprs_to_predicate_string(filters, &self.schema)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        // Build copy query.
        let query = format!(
            "SELECT {} FROM {} {} {} {}",
            projection_string, // SELECT <str>
            self.table,        // <table>
            // [WHERE]
            if predicate_string.is_empty() {
                ""
            } else {
                "WHERE "
            },
            predicate_string.as_str(), // <where-predicate>
            limit_string,              // [LIMIT ..]
        );

        Ok(Arc::new(SqliteQueryExec {
            query,
            state: self.state.clone(),
            schema: projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
}

#[derive(Debug)]
pub struct SqliteQueryExec {
    query: String,
    state: SqliteAccessState,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for SqliteQueryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for SqliteQueryExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid partition: {partition}"
            )));
        }

        let state = self.state.clone();
        let query = self.query.clone();
        let schema = self.schema.clone();

        let stream = async_stream::stream! {
            let mut stream = state.client.query(query);
            let conv = Converter::new(schema);

            while let Some(batch) = stream.next().await {
                let data = match batch {
                    Ok(b) => b.data,
                    Err(e) => {
                        yield Err(DataFusionError::Execution(e.to_string()));
                        return;
                    }
                };
                match conv.create_record_batch(data) {
                    Ok(rb) => {
                        yield Ok(rb);
                    },
                    Err(e) => {
                        yield Err(DataFusionError::Execution(e.to_string()));
                        return;
                    }
                };
            }
        };

        let stream = SqliteRecordBatchStream {
            schema: self.schema.clone(),
            inner: Box::pin(stream),
        };

        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            stream,
            partition,
            &self.metrics,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for SqliteQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteQueryExec")
    }
}

struct SqliteRecordBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
}

impl Stream for SqliteRecordBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for SqliteRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Convert filtering expressions to a predicate string usable with the
/// generated Postgres query.
fn exprs_to_predicate_string(exprs: &[Expr], schema: &Schema) -> Result<String> {
    let mut ss = Vec::new();
    let mut buf = String::new();
    for expr in exprs {
        if write_expr(expr, schema, &mut buf)? {
            ss.push(buf);
            buf = String::new();
        }
    }

    Ok(ss.join(" AND "))
}

/// Try to write the expression to the string, returning true if it was written.
fn write_expr(expr: &Expr, schema: &Schema, buf: &mut String) -> Result<bool> {
    match expr {
        Expr::Column(col) => {
            write!(buf, "{}", col)?;
        }
        Expr::Literal(val) => {
            util::encode_literal_to_text(util::Datasource::Sqlite, buf, val)?;
        }
        Expr::IsNull(expr) => {
            if write_expr(expr, schema, buf)? {
                write!(buf, " IS NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsNotNull(expr) => {
            if write_expr(expr, schema, buf)? {
                write!(buf, " IS NOT NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsTrue(expr) => {
            if write_expr(expr, schema, buf)? {
                write!(buf, " IS TRUE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsFalse(expr) => {
            if write_expr(expr, schema, buf)? {
                write!(buf, " IS FALSE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::BinaryExpr(binary) => {
            if should_skip_binary_expr(binary, schema)? {
                return Ok(false);
            }

            if !write_expr(binary.left.as_ref(), schema, buf)? {
                return Ok(false);
            }
            write!(buf, " {} ", binary.op)?;
            if !write_expr(binary.right.as_ref(), schema, buf)? {
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

fn should_skip_binary_expr(expr: &BinaryExpr, schema: &Schema) -> Result<bool> {
    fn is_not_supported_dt(expr: &Expr, schema: &Schema) -> Result<bool> {
        let data_type = match expr {
            Expr::Column(col) => {
                let field = schema.field_with_name(&col.name)?;
                field.data_type().clone()
            }
            Expr::Literal(scalar) => scalar.data_type(),
            _ => return Ok(false),
        };

        let supported = data_type.is_integer()
            || data_type.is_floating()
            || matches!(data_type, DataType::Utf8);
        Ok(!supported)
    }

    // Skip if we're trying to do any kind of binary op with text column
    Ok(is_not_supported_dt(&expr.left, schema)? || is_not_supported_dt(&expr.right, schema)?)
}
