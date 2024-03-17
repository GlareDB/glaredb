pub mod errors;

mod convert;
mod wrapper;

use std::any::Any;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use async_sqlite::rusqlite::types::Value;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{BinaryExpr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use protogen::metastore::types::options::StorageOptions;
use uuid::Uuid;

use self::errors::{Result, SqliteError};
use self::wrapper::SqliteAsyncClient;
use crate::common::url::DatasourceUrl;
use crate::common::util::{self, COUNT_SCHEMA};
use crate::lake::storage_options_into_store_access;
use crate::object_store::ObjStoreAccessor;

type DataFusionResult<T> = Result<T, DataFusionError>;

#[derive(Debug, Clone)]
pub struct SqliteAccess {
    pub db: PathBuf,
    pub cache: Option<Arc<tempfile::TempDir>>,
}

impl SqliteAccess {
    pub async fn new(url: DatasourceUrl, opts: Option<StorageOptions>) -> Result<Self> {
        match url {
            DatasourceUrl::File(ref location) => {
                if !location.try_exists()? {
                    Err(SqliteError::NoMatchingObjectFound {
                        url: url.clone(),
                        num: 0,
                    })
                } else {
                    Ok(Self {
                        db: location.clone(),
                        cache: None,
                    })
                }
            }
            DatasourceUrl::Url(_) => {
                let storage_options = match opts {
                    Some(v) => v,
                    None => {
                        return Err(SqliteError::Internal(
                            "storage options are required".to_string(),
                        ))
                    }
                };
                let store_access = storage_options_into_store_access(&url, &storage_options)?;

                let accessor = ObjStoreAccessor::new(store_access)?;
                let mut list = accessor.list_globbed(url.path()).await?;
                if list.len() != 1 {
                    return Err(SqliteError::NoMatchingObjectFound {
                        url,
                        num: list.len(),
                    });
                }

                let store = accessor.into_object_store();

                let obj = list.pop().unwrap().location;
                let payload = store.get(&obj).await?.bytes().await?;

                let tmpdir = Arc::new(
                    tempfile::Builder::new()
                        .prefix(
                            storage_options
                                .inner
                                .get("__tmp_prefix")
                                .unwrap_or_else(|| Uuid::new_v4()),
                        )
                        .rand_bytes(8)
                        .tempdir()?,
                );

                let tmpdir_path = tmpdir.path();
                let local_store =
                    object_store::local::LocalFileSystem::new_with_prefix(tmpdir_path)?;

                let local_path =
                    object_store::path::Path::parse(obj.filename().unwrap_or("sqlite"))?;

                local_store.put(&local_path, payload).await?;

                let db = tmpdir_path.join(local_path.filename().unwrap());

                Ok(Self {
                    db,
                    cache: Some(tmpdir.clone()),
                })
            }
        }
    }

    pub async fn connect(&self) -> Result<SqliteAccessState> {
        let client = SqliteAsyncClient::new(self.db.to_path_buf(), self.cache.clone()).await?;
        Ok(SqliteAccessState { client })
    }

    pub async fn validate_access(&self) -> Result<()> {
        let state = self.connect().await?;
        let _ = state.client.query_all("SELECT 1").await?;
        Ok(())
    }

    pub async fn validate_table_access(&self, table: &str) -> Result<()> {
        let state = self.connect().await?;
        state.validate_table_access(table).await
    }
}


#[derive(Clone, Debug)]
pub struct SqliteAccessState {
    client: SqliteAsyncClient,
}

impl SqliteAccessState {
    pub fn is_local_file(&self) -> bool {
        self.client.is_local_file()
    }

    async fn validate_table_access(&self, table: &str) -> Result<()> {
        let query = format!("SELECT * FROM {table} WHERE FALSE");
        let _ = self.client.query_all(query).await?;
        Ok(())
    }

    async fn get_table_schema(&self, table: &str) -> Result<Schema> {
        let batch = self
            .client
            .query_all(format!("PRAGMA table_info('{table}')"))
            .await?;

        let fields = (0..batch.data.len())
            .map(|row_idx| {
                let col_name = match batch.get_val_by_col_name(row_idx, "name") {
                    Some(Value::Text(s)) => s,
                    _ => unreachable!(),
                };

                let data_type = batch
                    .get_val_by_col_name(row_idx, "type")
                    .and_then(|ty| match ty {
                        Value::Text(s) => Some(s.to_ascii_lowercase()),
                        _ => None,
                    })
                    .and_then(|ty| match ty.as_str() {
                        "boolean" | "bool" => Some(DataType::Boolean),
                        "date" => Some(DataType::Date32),
                        "time" => Some(DataType::Time64(TimeUnit::Microsecond)),
                        "datetime" | "timestamp" => {
                            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                        }
                        s if s.contains("int") => Some(DataType::Int64),
                        s if s.contains("char") || s.contains("clob") || s.contains("text") => {
                            Some(DataType::Utf8)
                        }
                        s if s.contains("real") || s.contains("floa") || s.contains("doub") => {
                            Some(DataType::Float64)
                        }
                        s if s.contains("blob") => Some(DataType::Binary),
                        _ => None,
                    })
                    .unwrap_or(DataType::Utf8);

                let not_null = batch
                    .get_val_by_col_name(row_idx, "notnull")
                    .map(|v| match v {
                        Value::Integer(v) => *v != 0,
                        _ => false,
                    })
                    .unwrap_or_default();

                Field::new(col_name, data_type, !not_null)
            })
            .collect::<Vec<_>>();

        if fields.is_empty() {
            // Pragma doesn't error for non-existant tables. If we are unable
            // to fetch the schema for the table, we can try validating it to
            // ensure that it exists.
            self.validate_table_access(table).await?;
        }

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
            Some(projection) if !projection.is_empty() => {
                Arc::new(self.schema.project(projection)?)
            }
            _ => self.schema.clone(),
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

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if overwrite {
            return Err(DataFusionError::Execution("cannot overwrite".to_string()));
        }

        if !self.state.is_local_file() {
            return Err(DataFusionError::Execution(
                "cannot write remote file".to_string(),
            ));
        }

        Ok(Arc::new(SqliteInsertExec {
            input,
            table: self.table.to_string(),
            state: self.state.clone(),
            schema: self.schema.clone(),
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

        let stream = self.state.client.query(self.schema.clone(), &self.query);

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

#[derive(Debug)]
pub struct SqliteInsertExec {
    table: String,
    input: Arc<dyn ExecutionPlan>,
    state: SqliteAccessState,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for SqliteInsertExec {
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
                "cannot replace children for SqliteInsertExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid partition: {partition}"
            )));
        }
        let input = self.input.clone();
        let table = self.table.clone();
        let schema = self.schema.clone();
        let client = self.state.client.clone();

        let stream = futures::stream::once(async move {
            let column_names = Arc::new(
                schema
                    .fields
                    .into_iter()
                    .map(|field| field.name().to_owned())
                    .collect::<Vec<String>>()
                    .join(","),
            );
            // TODO: this entire operation in the scope of a
            // transaction.

            // this takes each incoming record batch, creates a single
            // insert statement and runs the insert. This means that
            // the returned count, depending on the number of input
            // record batches, may have multiple rows.
            //
            // this seems generally unlikely to occur in general use,
            // and is potentially correct anyway. (certainly,
            // preferable to the handling in the early-2024
            // implementation which builds one very large message and
            // sends it to the database in a single statement.)
            Ok::<_, DataFusionError>(
                execute_stream(input, ctx)?
                    .map(move |input_batch| {
                        let batch = input_batch?;

                        if batch.num_rows() == 0 {
                            return Err(DataFusionError::Execution(
                                "cannot insert empty value".to_string(),
                            ));
                        }

                        let mut stmt = String::default();
                        write!(
                            &mut stmt,
                            "INSERT INTO {} ({}) VALUES ",
                            table, column_names,
                        )?;

                        for row_idx in 0..batch.num_rows() {
                            let mut row_values = Vec::with_capacity(batch.num_columns());
                            for column in batch.columns() {
                                let mut buf = String::default();
                                util::encode_literal_to_text(
                                    util::Datasource::Sqlite,
                                    &mut buf,
                                    &ScalarValue::try_from_array(column.as_ref(), row_idx)
                                        .map_err(|e| DataFusionError::Execution(e.to_string()))?,
                                )
                                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                row_values.push(buf);
                            }

                            write!(
                                &mut stmt,
                                "{}({})",
                                if row_idx == 0 { "" } else { ", " },
                                row_values.join(",")
                            )?;
                        }
                        write!(&mut stmt, ";")?;
                        Ok(client.query(COUNT_SCHEMA.clone(), stmt))
                    })
                    .try_flatten(),
            )
        })
        .try_flatten();

        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            RecordBatchStreamAdapter::new(COUNT_SCHEMA.clone(), Box::pin(stream)),
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

impl DisplayAs for SqliteInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteInsertExec")
    }
}
