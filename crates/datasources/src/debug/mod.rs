//! A collection of debug datasources.
pub mod errors;
pub mod options;

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
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
use errors::DebugError;
use futures::Stream;
use parser::options::StatementOptions;
use protogen::metastore::types::options::{CredentialsOptions, TableOptions, TunnelOptions};

pub use self::options::{DebugTableType, TableOptionsDebug};
use crate::DatasourceError;


/// Validates if the tunnel is supported and returns whether a tunnel is going
/// to be used or not for  the connection.
pub fn validate_tunnel_connections(
    tunnel_opts: Option<&TunnelOptions>,
) -> Result<bool, DebugError> {
    match tunnel_opts {
        None => Ok(false),
        Some(TunnelOptions::Debug(_)) => Ok(true),
        Some(other) => Err(DebugError::InvalidTunnel(other.to_string())),
    }
}

pub struct DebugVirtualLister;

#[async_trait]
impl VirtualLister for DebugVirtualLister {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        Ok((0..2).map(|i| format!("schema_{i}")).collect())
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let tables = (0..2).map(|i| format!("{schema}_table_{i}")).collect();
        Ok(tables)
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        Ok((0..2)
            .map(|i| {
                let name = format!("{schema}_{table}_col_{i}");
                let datatype = if i % 2 == 0 {
                    DataType::Utf8
                } else {
                    DataType::Int64
                };
                let nullable = i % 2 == 0;
                Field::new(name, datatype, nullable)
            })
            .collect())
    }
}

pub struct DebugTableProvider {
    pub typ: DebugTableType,
    pub tunnel: bool,
}

#[async_trait]
impl TableProvider for DebugTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::new(self.typ.arrow_schema())
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
        // Ensure valid projection before returning exec.
        let _ = self.typ.projected_arrow_schema(projection)?;

        Ok(Arc::new(DebugTableExec {
            typ: self.typ.clone(),
            projection: projection.cloned(),
            limit,
            tunnel: self.tunnel,
        }))
    }
}

#[derive(Debug)]
struct DebugTableExec {
    typ: DebugTableType,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    tunnel: bool,
}

impl ExecutionPlan for DebugTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::new(
            self.typ
                .projected_arrow_schema(self.projection.as_ref())
                .unwrap(),
        )
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
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for DebugTableExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        Ok(match &self.typ {
            DebugTableType::ErrorDuringExecution => Box::pin(AlwaysErrorStream {
                arrow_schema: self.schema(),
            }),
            DebugTableType::NeverEnding => Box::pin(NeverEndingStream {
                batch: self
                    .typ
                    .projected_record_batch(self.tunnel, self.projection.as_ref())
                    .unwrap(),
                curr_count: 0,
                limit: self.limit,
            }),
        })
    }

    fn statistics(&self) -> DatafusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for DebugTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DebugTableExec: type={}", self.typ.as_str())
    }
}

#[derive(Debug, Clone)]
struct AlwaysErrorStream {
    arrow_schema: ArrowSchemaRef,
}

impl Stream for AlwaysErrorStream {
    type Item = DatafusionResult<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(Err(DataFusionError::External(Box::new(
            DebugError::ExecutionError("always error"),
        )))))
    }
}

impl RecordBatchStream for AlwaysErrorStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }
}

#[derive(Debug, Clone)]
struct NeverEndingStream {
    batch: RecordBatch,
    curr_count: usize,
    limit: Option<usize>,
}

impl Stream for NeverEndingStream {
    type Item = DatafusionResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(limit) = self.limit {
            if self.curr_count > limit {
                return Poll::Ready(None);
            }
        }
        let batch = self.batch.clone();
        self.curr_count += batch.num_rows();
        Poll::Ready(Some(Ok(batch)))
    }
}

impl RecordBatchStream for NeverEndingStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.batch.schema()
    }
}
pub struct DebugDatasource;


#[async_trait]
impl crate::Datasource for DebugDatasource {
    fn name(&self) -> &'static str {
        "debug"
    }

    fn table_options_from_stmt(
        &self,
        opts: &mut StatementOptions,
        creds: Option<CredentialsOptions>,
        tunnel_opts: Option<TunnelOptions>,
    ) -> Result<TableOptions, DatasourceError> {
        validate_tunnel_connections(tunnel_opts.as_ref()).unwrap();

        let typ: Option<DebugTableType> = match creds {
            Some(CredentialsOptions::Debug(c)) => c.table_type.parse().ok(),
            Some(other) => unreachable!("invalid credentials {other} for debug datasource"),
            None => None,
        };
        let typ: DebugTableType = opts
            .remove_required_or("table_type", typ)
            .map_err(|e| DebugError::UnknownDebugTableType(e.to_string()))?;

        Ok(TableOptionsDebug { table_type: typ }.into())
    }

    async fn create_table_provider(
        &self,
        options: &TableOptions,
        tunnel_opts: Option<&TunnelOptions>,
    ) -> Result<Arc<dyn TableProvider>, DatasourceError> {
        let options = TableOptionsDebug::try_from(options).unwrap();

        Ok(Arc::new(DebugTableProvider {
            typ: options.table_type,
            tunnel: tunnel_opts.is_some(),
        }))
    }
}
