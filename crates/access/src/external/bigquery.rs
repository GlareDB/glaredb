//! BigQuery external table implementation.
use crate::errors::{internal, AccessError, Result};
use async_stream::{stream, AsyncStream};
use async_trait::async_trait;
use bigquery_storage::yup_oauth2::{
    authenticator::{DefaultHyperClient, HyperClientBuilder},
    ServiceAccountAuthenticator,
};
use bigquery_storage::{BufferedArrowIpcReader, Client, ReadSession};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::ipc::reader::StreamReader as ArrowStreamReader;
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
use gcp_bigquery_client::Client as BigQueryClient;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

// Convenience type aliases.
type DefaultConnector = <DefaultHyperClient as HyperClientBuilder>::Connector;
type BigQueryStorage = Client<DefaultConnector>;

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
    /// Client for getting the metadata for a table.
    metadata: BigQueryClient,
}

impl BigQueryAccessor {
    /// Connect to the bigquery instance.
    pub async fn connect(access: BigQueryTableAccess) -> Result<Self> {
        // TODO: We end up deserializing the key twice. Once for this client,
        // and again for the storage client during query execution.
        let metadata = {
            let key = serde_json::from_str(&access.gcp_service_acccount_key_json)?;
            BigQueryClient::from_service_account_key(key, true).await?
        };

        Ok(BigQueryAccessor { access, metadata })
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<BigQueryTableProvider> {
        let table_meta = self
            .metadata
            .table()
            .get(
                &self.access.gpc_project_id,
                &self.access.dataset_id,
                &self.access.table_id,
                None,
            )
            .await?;
        let arrow_schema = bigquery_table_to_arrow_schema(&table_meta)?;

        Ok(BigQueryTableProvider {
            access: self.access,
            predicate_pushdown,
            arrow_schema: Arc::new(arrow_schema),
        })
    }
}

pub struct BigQueryTableProvider {
    access: BigQueryTableAccess,
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
        // let projected_schema = match projection {
        //     Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
        //     None => self.arrow_schema.clone(),
        // };

        // TODO: Duplicated key deserialization.
        let mut storage = {
            let key = serde_json::from_str(&self.access.gcp_service_acccount_key_json)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let sa = ServiceAccountAuthenticator::builder(key).build().await?;
            BigQueryStorage::new(sa)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        let mut builder = storage.read_session_builder(bigquery_storage::Table::new(
            &self.access.gpc_project_id,
            &self.access.dataset_id,
            &self.access.table_id,
        ));

        // Add row restriction.
        // TODO: Check what restrictions are valid.
        if self.predicate_pushdown {
            let restriction = filters
                .iter()
                .map(|expr| expr.to_string())
                .collect::<Vec<_>>()
                .join(" AND ");
            builder = builder.row_restriction(restriction);
        }

        let mut sess = builder
            .build()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut streams = Vec::new();
        while let Some(stream) = sess
            .next_stream()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            streams.push(Mutex::new(Some(stream)));
        }

        Ok(Arc::new(BigQueryExec {
            arrow_schema: self.arrow_schema.clone(),
            streams,
        }))
    }
}

struct BigQueryExec {
    arrow_schema: ArrowSchemaRef,
    /// Open streams to BigQuery storage.
    ///
    /// Note that streams are consumed during execution.
    streams: Vec<Mutex<Option<BufferedArrowIpcReader>>>,
}

impl ExecutionPlan for BigQueryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.streams.len())
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let reader = {
            let reader = self.streams.get(partition).ok_or_else(|| {
                DataFusionError::Execution(format!("missing stream for partion: {}", partition))
            })?;
            let mut reader = reader.lock().unwrap();
            // Replace the reader, disallowing execution of the same partition.
            reader.take().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "attempted to read the same partition multiple times: {}",
                    partition
                ))
            })?
        };

        Ok(Box::pin(BufferedIpcStream::new(self.schema(), reader)))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BigQueryExec: schema={}", self.arrow_schema)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl fmt::Debug for BigQueryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigQueryExec")
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

struct BufferedIpcStream {
    schema: ArrowSchemaRef,
    inner: Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>> + Send>>,
}

impl BufferedIpcStream {
    fn new(schema: ArrowSchemaRef, reader: BufferedArrowIpcReader) -> Self {
        let stream = stream! {
            let buf = match reader.into_vec().await {
                Ok(buf) => buf,
                Err(e) => {
                    yield Err(ArrowError::ExternalError(Box::new(e)));
                    return;
                },
            };
            let mut reader = ArrowStreamReader::try_new(Cursor::new(buf), None)?;
            while let Some(batch) = reader.next() {
                let batch = batch?;
                yield Ok(batch);
            }
        };

        BufferedIpcStream {
            schema,
            inner: Box::pin(stream),
        }
    }
}

impl Stream for BufferedIpcStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for BufferedIpcStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
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
    // See <https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details>
    // for how BigQuery types map to Arrow types.
    for field in fields {
        let arrow_typ = match &field.r#type {
            FieldType::Bool | FieldType::Boolean => DataType::Boolean,
            FieldType::String => DataType::Utf8,
            FieldType::Integer => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::Float => DataType::Float32,
            FieldType::Float64 => DataType::Float64,
            FieldType::Bytes => DataType::Binary,
            FieldType::Date => DataType::Date32,
            other => return Err(internal!("unsupported bigquery type: {:?}", other)),
        };

        let field = Field::new(&field.name, arrow_typ, true);
        arrow_fields.push(field);
    }

    Ok(ArrowSchema::new(arrow_fields))
}
