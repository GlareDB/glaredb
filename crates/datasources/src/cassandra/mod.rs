mod builder;
mod errors;
mod exec;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef,
    Float32Builder,
    Float64Builder,
    Int16Builder,
    Int32Builder,
    Int8Builder,
    StringBuilder,
    TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{
    DataType,
    Field,
    Fields,
    Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
    TimeUnit,
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
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    RecordBatchStream,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
pub use errors::*;
use futures::{Stream, StreamExt};
use scylla::frame::response::result::{ColumnType, CqlValue, Row};
use scylla::transport::session::Session;
use scylla::SessionBuilder;

use self::exec::CassandraExec;

pub struct CassandraAccess {
    host: String,
}
impl CassandraAccess {
    pub fn new(host: String) -> Self {
        Self { host }
    }
    pub async fn validate_access(&self) -> Result<()> {
        let _access = CassandraAccessState::try_new(&self.host).await?;

        Ok(())
    }
    pub async fn connect(&self) -> Result<CassandraAccessState> {
        CassandraAccessState::try_new(&self.host).await
    }
}
pub struct CassandraAccessState {
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
fn try_convert_dtype_string(ty: &str) -> Result<DataType> {
    Ok(match ty {
        "custom" => return Err(CassandraError::UnsupportedDataType(ty.to_string())),
        "ascii" => DataType::Utf8,
        "date" => DataType::Date64,
        "double" => DataType::Float64,
        "duration" => DataType::Duration(TimeUnit::Nanosecond),
        "float" => DataType::Float32,
        "int" => DataType::Int32,
        "text" => DataType::Utf8,
        "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "smallint" => DataType::Int16,
        "tinyint" => DataType::Int8,
        "uuid" => DataType::Utf8,
        "bigint" => DataType::Int64,
        // list<T>
        lst if lst.contains('<') => {
            // get the inner type from "list<{inner}>"
            let inner = lst.split('<').nth(1).and_then(|s| s.split('>').next());

            match inner {
                Some(inner) => DataType::new_list(try_convert_dtype_string(inner)?, true),
                None => return Err(CassandraError::UnsupportedDataType(ty.to_string())),
            }
        }
        _ => return Err(CassandraError::UnsupportedDataType(ty.to_string())),
    })
}

impl CassandraAccessState {
    pub async fn try_new(host: impl AsRef<str>) -> Result<Self> {
        let session = SessionBuilder::new()
            .known_node(host)
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
    pub async fn try_new(host: String, ks: String, table: String) -> Result<Self> {
        let access = CassandraAccessState::try_new(host).await?;
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

#[async_trait]
impl VirtualLister for CassandraAccessState {
    /// List schemas for a data source.
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        let query = "SELECT keyspace_name FROM system_schema.keyspaces";
        self.session
            .query(query, &[])
            .await
            .map_err(CassandraError::from)
            .and_then(|res| {
                res.rows_or_empty()
                    .into_iter()
                    .map(|row| match row.columns[0] {
                        Some(CqlValue::Text(ref s)) => Ok(s.clone()),
                        _ => Err(CassandraError::String("invalid schema".to_string())),
                    })
                    .collect::<Result<Vec<String>>>()
            })
            .map_err(ExtensionError::access)
    }

    /// List tables for a data source.
    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let query = format!(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
            schema
        );
        self.session
            .query(query, &[])
            .await
            .map_err(CassandraError::from)
            .and_then(|res| {
                res.rows_or_empty()
                    .into_iter()
                    .map(|row| match row.columns[0] {
                        Some(CqlValue::Text(ref s)) => Ok(s.clone()),
                        _ => Err(CassandraError::String("invalid table".to_string())),
                    })
                    .collect::<Result<Vec<String>>>()
            })
            .map_err(ExtensionError::access)
    }

    /// List columns for a specific table in the datasource.
    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        let query = format!(
            "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}'",
            schema, table
        );
        self.session
            .query(query, &[])
            .await
            .map_err(CassandraError::from)
            .and_then(|res| {
                res.rows_or_empty()
                    .into_iter()
                    .map(|row| {
                        let name = match row.columns[0] {
                            Some(CqlValue::Text(ref s)) => s.clone(),
                            _ => return Err(CassandraError::String("invalid column".to_string())),
                        };
                        let ty = match row.columns[1] {
                            Some(CqlValue::Text(ref s)) => s.clone(),
                            _ => return Err(CassandraError::String("invalid column".to_string())),
                        };
                        let dtype = try_convert_dtype_string(&ty)?;

                        Ok(Field::new(name, dtype, true))
                    })
                    .collect::<Result<_>>()
            })
            .map_err(ExtensionError::access)
    }
}
