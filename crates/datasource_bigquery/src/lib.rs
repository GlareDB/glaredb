//! BigQuery external table implementation.
pub mod errors;

use crate::errors::{BigQueryError, Result};
use async_channel::Receiver;
use async_stream::stream;
use async_trait::async_trait;
use bigquery_storage::yup_oauth2::{
    authenticator::{DefaultHyperClient, HyperClientBuilder},
    ServiceAccountAuthenticator,
};
use bigquery_storage::{BufferedArrowIpcReader, Client};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::ipc::reader::StreamReader as ArrowStreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datasource_common::util;
use futures::{Stream, StreamExt};
use gcp_bigquery_client::Client as BigQueryClient;
use gcp_bigquery_client::{
    model::{field_type::FieldType, table::Table},
    project::GetOptions,
};
use metastore::types::catalog::ConnectionOptionsBigQuery;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{self, Write};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
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
    pub gcp_project_id: String,
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

    /// Validate big query connection
    pub async fn validate_connection(options: &ConnectionOptionsBigQuery) -> Result<()> {
        let client = {
            let key = serde_json::from_str(&options.service_account_key)?;
            BigQueryClient::from_service_account_key(key, true).await?
        };

        let project_list = client.project().list(GetOptions::default()).await?;

        project_list
            .projects
            .iter()
            .flatten()
            .flat_map(|p| &p.id)
            .find(|p| p.as_str() == options.project_id.as_str())
            .ok_or(BigQueryError::ProjectReadPerm(
                options.project_id.to_owned(),
            ))?;

        Ok(())
    }

    /// Validate big query connection and access to table
    pub async fn validate_table_access(access: &BigQueryTableAccess) -> Result<()> {
        let client = {
            let key = serde_json::from_str(&access.gcp_service_acccount_key_json)?;
            let sa = ServiceAccountAuthenticator::builder(key)
                .build()
                .await
                .map_err(BigQueryError::AuthKey)?;
            BigQueryStorage::new(sa).await?
        };

        let table = bigquery_storage::Table::new(
            &access.gcp_project_id,
            &access.dataset_id,
            &access.table_id,
        );

        client
            .read_session_builder(table)
            .row_restriction("false".to_string())
            .build()
            .await?;

        Ok(())
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<BigQueryTableProvider> {
        let table_meta = self
            .metadata
            .table()
            .get(
                &self.access.gcp_project_id,
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
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // TODO: Fix duplicated key deserialization.
        let storage = {
            let key = serde_json::from_str(&self.access.gcp_service_acccount_key_json)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let sa = ServiceAccountAuthenticator::builder(key).build().await?;
            BigQueryStorage::new(sa)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        // Projection.
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        let mut builder = storage.read_session_builder(bigquery_storage::Table::new(
            &self.access.gcp_project_id,
            &self.access.dataset_id,
            &self.access.table_id,
        ));

        // Add row restriction.
        // TODO: Check what restrictions are valid.
        let predicate = if self.predicate_pushdown {
            let restriction = exprs_to_predicate_string(filters)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            builder = builder.row_restriction(restriction.clone());
            restriction
        } else {
            String::new()
        };

        // Select fields based off of what's in our projected schema.
        let selected: Vec<_> = projected_schema
            .fields
            .iter()
            .map(|field| field.name().clone())
            .collect();
        builder = builder.selected_fields(selected);

        let mut sess = builder
            .build()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let num_partitions = sess.len_streams();

        let (send, recv) = async_channel::bounded(num_partitions);
        tokio::spawn(async move {
            loop {
                let stream_opt = {
                    match sess.next_stream().await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(%e, "unable to fetch next stream");
                            break;
                        }
                    }
                };
                if let Some(stream) = stream_opt {
                    match send.send(stream).await {
                        Ok(_) => {}
                        Err(_e /* : closed or full channel error */) => {
                            tracing::error!(
                                "cannot send stream over the buffered channel [programming error]"
                            );
                            break;
                        }
                    };
                } else {
                    // Received `None`. No more streams to send into the channel.
                    break;
                }
            }
            // Close the channel once everything's done!
            send.close();
        });

        Ok(Arc::new(BigQueryExec {
            predicate,
            arrow_schema: projected_schema,
            receiver: RwLock::new(recv),
            num_partitions,
        }))
    }
}

struct BigQueryExec {
    predicate: String,
    arrow_schema: ArrowSchemaRef,
    receiver: RwLock<Receiver<BufferedArrowIpcReader>>,
    num_partitions: usize,
}

impl ExecutionPlan for BigQueryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.num_partitions)
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
        let recv = {
            let guard = self.receiver.read();
            Receiver::clone(&guard)
        };

        Ok(Box::pin(BufferedIpcStream::new(
            self.schema(),
            recv,
            partition,
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BigQueryExec: predicate={}",
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

impl fmt::Debug for BigQueryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigQueryExec")
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

struct BufferedIpcStream {
    schema: ArrowSchemaRef,
    inner: Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>,
}

impl BufferedIpcStream {
    fn new(
        schema: ArrowSchemaRef,
        receiver: Receiver<BufferedArrowIpcReader>,
        partition: usize,
    ) -> Self {
        let stream = stream! {
            let reader = match receiver.recv().await {
                Ok(r) => r,
                Err(_e /* : closed channel error */) => {
                    yield Err(DataFusionError::Execution(format!("missing stream for partition: {}", partition)));
                    return;
                }
            };
            let buf = match reader.into_vec().await {
                Ok(buf) => buf,
                Err(e) => {
                    yield Err(DataFusionError::External(Box::new(e)));
                    return;
                },
            };
            let reader = ArrowStreamReader::try_new(Cursor::new(buf), None)?;
            for batch in reader {
                let batch = batch?;
                let batch = util::normalize_batch(&batch)?;
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
    type Item = DatafusionResult<RecordBatch>;

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
        .ok_or(BigQueryError::UnknownFieldsForTable)?;

    let mut arrow_fields = Vec::with_capacity(fields.len());
    // See <https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details>
    // for how BigQuery types map to Arrow types.
    for field in fields {
        let arrow_typ = match &field.r#type {
            FieldType::Bool | FieldType::Boolean => DataType::Boolean,
            FieldType::String => DataType::Utf8,
            FieldType::Integer | FieldType::Int64 => DataType::Int64,
            FieldType::Float | FieldType::Float64 => DataType::Float64,
            FieldType::Bytes => DataType::Binary,
            FieldType::Date => DataType::Date32,
            FieldType::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, None),
            FieldType::Timestamp => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned()))
            }
            FieldType::Time => DataType::Time64(TimeUnit::Nanosecond),
            FieldType::Numeric => DataType::Decimal128(38, 9),
            // TODO: Bignumeric throws an error which terminates the connection abruptly.
            // FieldType::Bignumeric => DataType::Decimal256(76, 38),
            FieldType::Geography => DataType::Utf8,
            other => return Err(BigQueryError::UnsupportedBigQueryType(other.clone())),
        };

        let field = Field::new(&field.name, arrow_typ, true);
        arrow_fields.push(field);
    }

    Ok(ArrowSchema::new(arrow_fields))
}

/// Convert filtering expressions to a predicate string usable with BigQuery's
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
            // Handled by "IS NULL" ...
            assert!(!val.is_null());
            // Handled by "IS TRUE" ...
            assert!(!matches!(val, ScalarValue::Boolean(_)));

            util::encode_literal_to_text(buf, val)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{BinaryExpr, Operator};

    #[test]
    fn valid_expr_string() {
        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "a".to_string(),
                })),
                op: Operator::Lt,
                right: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "b".to_string(),
                })),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "c".to_string(),
                })),
                op: Operator::Lt,
                right: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "d".to_string(),
                })),
            }),
        ];

        let out = exprs_to_predicate_string(&exprs).unwrap();
        assert_eq!(out, "a < b AND c < d")
    }

    #[test]
    fn skip_unsupported_expr_string() {
        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "a".to_string(),
                })),
                op: Operator::Lt,
                right: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "b".to_string(),
                })),
            }),
            // BigQuery doesn't support sorting expressions in the row
            // restriction.
            Expr::Sort(Sort {
                expr: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "a".to_string(),
                })),
                asc: true,
                nulls_first: true,
            }),
        ];

        let out = exprs_to_predicate_string(&exprs).unwrap();
        assert_eq!(out, "a < b")
    }
}
