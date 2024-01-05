mod builder;
mod errors;
mod exec;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int8Builder,
    StringBuilder, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
pub use errors::*;
use futures::Stream;
use futures::StreamExt;
use scylla::frame::response::result::{ColumnType, CqlValue, Row};

use scylla::transport::session::Session;
use scylla::SessionBuilder;
use std::any::Any;

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use self::exec::CassandraExec;

pub struct CassandraAccess {
    session: Session,
}

fn try_convert_dtype(ty: &ColumnType) -> Result<DataType> {
    Ok(match ty {
        ColumnType::Ascii => DataType::Utf8,
        ColumnType::Date => DataType::Date64,
        ColumnType::Double => DataType::Float64,
        ColumnType::Duration => DataType::Duration(TimeUnit::Nanosecond),
        ColumnType::Float => DataType::Float32,
        ColumnType::Int => DataType::Int32,
        ColumnType::Text => DataType::Utf8,
        ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
        ColumnType::SmallInt => DataType::Int16,
        ColumnType::TinyInt => DataType::Int8,
        ColumnType::Uuid => DataType::Utf8,
        ColumnType::BigInt => DataType::Int64,
        ColumnType::List(inner) | ColumnType::Set(inner) => {
            let inner = try_convert_dtype(inner)?;
            DataType::new_list(inner, true)
        }
        _ => return Err(CassandraError::UnsupportedDataType(format!("{:?}", ty))),
    })
}

impl CassandraAccess {
    pub async fn try_new(conn_str: impl AsRef<str>) -> Result<Self> {
        let session = SessionBuilder::new()
            .known_node(conn_str)
            .connection_timeout(Duration::from_secs(10))
            .build()
            .await?;
        Ok(Self { session })
    }
    async fn get_schema(&self, ks: &str, table: &str) -> Result<ArrowSchema> {
        let query = format!("SELECT * FROM {ks}.{table} LIMIT 1");
        let res = self.session.query(query, &[]).await?;
        let fields: Fields = res
            .col_specs
            .into_iter()
            .map(|c| {
                let name = c.name;
                let ty = c.typ;
                let dtype = try_convert_dtype(&ty)?;
                Ok(Field::new(name, dtype, true))
            })
            .collect::<Result<_>>()?;
        Ok(ArrowSchema::new(fields))
    }
    pub async fn validate_table_access(&self, ks: &str, table: &str) -> Result<()> {
        let query = format!("SELECT * FROM {ks}.{table} LIMIT 1");
        let res = self.session.query(query, &[]).await?;
        if res.col_specs.is_empty() {
            return Err(CassandraError::TableNotFound(format!(
                "table {} not found in keyspace {}",
                table, ks
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CassandraTableProvider {
    schema: Arc<ArrowSchema>,
    ks: String,
    table: String,
    session: Arc<Session>,
}

impl CassandraTableProvider {
    pub async fn try_new(conn_str: String, ks: String, table: String) -> Result<Self> {
        let access = CassandraAccess::try_new(conn_str).await?;
        let schema = access.get_schema(&ks, &table).await?;
        Ok(Self {
            schema: Arc::new(schema),
            session: Arc::new(access.session),
            ks,
            table,
        })
    }
}

#[async_trait]
impl TableProvider for CassandraTableProvider {
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
        let mut query = format!(
            "SELECT {} FROM {}.{}",
            projection_string, self.ks, self.table
        );
        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        let exec = CassandraExec::new(projected_schema, query, self.session.clone());
        Ok(Arc::new(exec))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "inserts not yet supported for Cassandra".to_string(),
        ))
    }
}
