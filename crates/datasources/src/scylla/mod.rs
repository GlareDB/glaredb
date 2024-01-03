mod errors;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
pub use errors::*;
use scylla::frame::response::result::ColumnType;
use scylla::macros::FromRow;
use scylla::transport::session::{IntoTypedRows, Session};
use scylla::SessionBuilder;
use std::any::Any;

use std::fmt;
use std::sync::Arc;

struct ScyllaAccess {
    session: Session,
}

fn convert_dtype(ty: &ColumnType) -> DataType {
    match ty {
        ColumnType::Custom(_) => todo!(),
        ColumnType::Ascii => DataType::Utf8,
        ColumnType::Boolean => todo!(),
        ColumnType::Blob => todo!(),
        ColumnType::Counter => todo!(),
        ColumnType::Date => DataType::Date64,
        ColumnType::Decimal => todo!(),
        ColumnType::Double => DataType::Float64,
        ColumnType::Duration => DataType::Duration(TimeUnit::Millisecond),
        ColumnType::Float => DataType::Float32,
        ColumnType::Int => DataType::Int32,
        ColumnType::BigInt => todo!(),
        ColumnType::Text => DataType::Utf8,
        ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
        ColumnType::Inet => todo!(),
        ColumnType::List(_) => todo!(),
        ColumnType::Map(_, _) => todo!(),
        ColumnType::Set(_) => todo!(),
        ColumnType::UserDefinedType { .. } => todo!(),
        ColumnType::SmallInt => DataType::Int16,
        ColumnType::TinyInt => DataType::Int8,
        ColumnType::Time => todo!(),
        ColumnType::Timeuuid => todo!(),
        ColumnType::Tuple(_inner) => todo!(),
        ColumnType::Uuid => DataType::Utf8,
        ColumnType::Varint => todo!(),
    }
}
impl ScyllaAccess {
    pub async fn try_new(conn_str: impl AsRef<str>) -> Result<Self> {
        let session = SessionBuilder::new().known_node(conn_str).build().await?;
        Ok(Self { session })
    }
    async fn get_schema(&self, table: &str) -> Result<ArrowSchema> {
        let query = format!("SELECT * FROM {table} LIMIT 1");
        let res = self.session.query(query, &[]).await?;
        let fields: Fields = res
            .col_specs
            .into_iter()
            .map(|c| {
                let name = c.name;
                let ty = c.typ;
                let dtype = convert_dtype(&ty);

                Field::new(name, dtype, true)
            })
            .collect();
        let s = ArrowSchema::new(fields);
        println!("{:?}", s);
        Ok(s)
    }
}

#[derive(Debug, Clone)]
pub struct ScyllaTableProvider {
    schema: Arc<ArrowSchema>,
    table: String,
    session: Arc<Session>,
}

impl ScyllaTableProvider {
    pub async fn try_new(conn_str: &str, table: &str) -> Result<Self> {
        let access = ScyllaAccess::try_new(conn_str).await?;
        let schema = access.get_schema(table).await?;
        Ok(Self {
            schema: Arc::new(schema),
            session: Arc::new(access.session),
            table: table.to_string(),
        })
    }
}

#[async_trait]
impl TableProvider for ScyllaTableProvider {
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
        _filters: &[Expr],
        _limit: Option<usize>,
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
        let query = format!("SELECT {} FROM {}", projection_string, "table");

        let exec = ScyllaExec::new(projected_schema, query, self.session.clone());
        Ok(Arc::new(exec))
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

struct ScyllaExec {
    schema: ArrowSchemaRef,
    session: Arc<Session>,
    query: String,
    metrics: ExecutionPlanMetricsSet,
}

impl ScyllaExec {
    fn new(schema: ArrowSchemaRef, query: String, session: Arc<Session>) -> ScyllaExec {
        ScyllaExec {
            schema,
            session,
            query,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}
impl ExecutionPlan for ScyllaExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
    fn output_partitioning(&self) -> Partitioning {
        // TODO: does scylla driver support partitioning?
        Partitioning::UnknownPartitioning(1)
    }
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for ScyllaExec".to_string(),
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        todo!("execute scylla query")
    }
    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for ScyllaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ScyllaExec")
    }
}

impl fmt::Debug for ScyllaExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaExec")
            .field("schema", &self.schema)
            .field("query", &self.query)
            .finish_non_exhaustive()
    }
}
