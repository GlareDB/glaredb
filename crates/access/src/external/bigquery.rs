use crate::errors::{internal, AccessError, Result};
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
use gcp_bigquery_client::model::{
    field_type::FieldType, table::Table, table_field_schema::TableFieldSchema,
};
use gcp_bigquery_client::Client;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;

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

        unimplemented!()
    }
}

pub struct BigQueryTableProvider {
    predicate_pushdown: bool,
}

#[async_trait]
impl TableProvider for BigQueryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        unimplemented!()
        // self.arrow_schema.clone()
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
        unimplemented!()
    }
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
