//! BigQuery external table implementation.
//!
//! This is currently using the record based api, eventually we'll want to
//! switch to <https://cloud.google.com/bigquery/docs/reference/storage>.
use crate::errors::{internal, AccessError, Result};
use async_stream::{stream, AsyncStream};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::Statistics;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::{
    field_type::FieldType, job_configuration_query::JobConfigurationQuery, table::Table,
    table_field_schema::TableFieldSchema, table_row::TableRow,
};
use gcp_bigquery_client::Client;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Information needed to access an external BigQuery table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BigQueryTableAccess {
    /// Service account key for accessing BigQuery.
    ///
    /// The service account should have 'BigQuery Data Viewer' and 'BigQuery Job
    /// User' permissions.
    pub gcp_service_acccount_key_json: String,
    pub gpc_project_id: String,
    pub dataset_id: String,
    pub table_id: String,
}

pub struct BigQueryAccessor {
    access: BigQueryTableAccess,
    client: Client,
}

impl BigQueryAccessor {
    /// Connect to the bigquery instance.
    pub async fn connect(access: BigQueryTableAccess) -> Result<Self> {
        let sa = serde_json::from_str(&access.gcp_service_acccount_key_json)?;
        let client = Client::from_service_account_key(sa, true).await?;
        Ok(BigQueryAccessor { access, client })
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<BigQueryTableProvider> {
        let table = self
            .client
            .table()
            .get(
                &self.access.gpc_project_id,
                &self.access.dataset_id,
                &self.access.table_id,
                None,
            )
            .await?;

        let arrow_schema = bigquery_table_to_arrow_schema(&table)?;

        Ok(BigQueryTableProvider {
            access: self.access,
            client: self.client,
            predicate_pushdown,
            arrow_schema: Arc::new(arrow_schema),
        })
    }
}

pub struct BigQueryTableProvider {
    access: BigQueryTableAccess,
    client: Client,
    predicate_pushdown: bool,
    arrow_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for BigQueryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // Projection.
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        let query = query_string(
            &projected_schema,
            filters,
            limit,
            self.predicate_pushdown,
            &self.access.gpc_project_id,
            &self.access.dataset_id,
            &self.access.table_id,
        );

        // Kick off the query job.
        let stream = self.client.job().query_all(
            &self.access.gpc_project_id,
            JobConfigurationQuery {
                query,
                ..Default::default()
            },
            None,
        );

        unimplemented!()
    }
}

#[derive(Debug)]
struct BigQueryExec {
    arrow_schema: ArrowSchemaRef,
}

impl ExecutionPlan for BigQueryExec {
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
            "cannot replace children for BigQueryExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        unimplemented!()
        // let stream = ChunkStream {
        //     state: StreamState::Idle,
        //     types: self.pg_types.clone(),
        //     opener: self.opener.clone(),
        //     arrow_schema: self.arrow_schema.clone(),
        // };
        // Ok(Box::pin(stream))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BigQueryExec: schema={}", self.arrow_schema)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// Convert BigQuery rows to Arrow record batches.
struct RowAdapterStream {
    schema: ArrowSchemaRef,
    stream: Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>>>>,
}

impl RowAdapterStream {
    fn new(
        schema: ArrowSchemaRef,
        row_stream: Pin<Box<dyn Stream<Item = Result<Vec<TableRow>, BQError>>>>,
    ) -> RowAdapterStream {
        let stream_schema = schema.clone();
        let stream = stream! {
            yield Ok(RecordBatch::new_empty(stream_schema));
        };
        RowAdapterStream {
            schema,
            stream: stream.boxed(),
        }
    }
}

impl Stream for RowAdapterStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

/// Build a SQL query string to send to BigQuery.
fn query_string(
    projected_schema: &ArrowSchema,
    filters: &[Expr],
    limit: Option<usize>,
    predicate_pushdown: bool,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> String {
    // SELECT ...
    let projection_string = projected_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>()
        .join(",");

    // LIMIT
    let limit_string = match limit {
        Some(limit) => format!("LIMIT {}", limit),
        None => String::new(),
    };

    // WHERE
    //
    // TODO: This may produce an invalid clause. We'll likely only want to
    // convert some predicates.
    let predicate_string = {
        if predicate_pushdown {
            let s = filters
                .iter()
                .map(|expr| expr.to_string())
                .collect::<Vec<_>>()
                .join(" AND ");
            if s.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", s)
            }
        } else {
            String::new()
        }
    };

    let query = format!(
        "SELECT {} FROM {}.{}.{} {} {}",
        projection_string, project_id, dataset_id, table_id, predicate_string, limit_string
    );

    query
}

/// Try to convert a bigquery table definition to an arrow schema.
fn bigquery_table_to_arrow_schema(table: &Table) -> Result<ArrowSchema> {
    let fields = table
        .schema
        .fields
        .as_ref()
        .ok_or_else(|| internal!("unknown fields in bigquery table"))?;

    let mut arrow_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let arrow_typ = match &field.r#type {
            FieldType::Bool | FieldType::Boolean => DataType::Boolean,
            FieldType::String => DataType::Utf8,
            FieldType::Integer => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::Float => DataType::Float32,
            FieldType::Float64 => DataType::Float64,
            FieldType::Bytes => DataType::Binary,
            FieldType::Date => DataType::Timestamp(TimeUnit::Second, None), // TODO: Check the date types.
            other => return Err(internal!("unsupported bigquery type: {:?}", other)),
        };

        let field = Field::new(&field.name, arrow_typ, true);
        arrow_fields.push(field);
    }

    Ok(ArrowSchema::new(arrow_fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_string_simple() {
        let fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ];
        let schema = ArrowSchema::new(fields);

        let query = query_string(
            &schema,
            &[],
            None,
            true,
            "test_project",
            "test_dataset",
            "test_table",
        );

        assert_eq!(
            "SELECT a,b FROM test_project.test_dataset.test_table",
            query.trim(),
        );
    }
}
