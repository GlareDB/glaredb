//! A collection of debug datasources.
pub mod errors;

use async_trait::async_trait;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use errors::DebugError;
use futures::Stream;
use protogen::metastore::types::options::TunnelOptions;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DebugTableType {
    /// A table that will always return an error on the record batch stream.
    ErrorDuringExecution,
    /// A table that never stops sending record batches.
    NeverEnding,
}

impl fmt::Display for DebugTableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for DebugTableType {
    type Err = DebugError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "error_during_execution" => DebugTableType::ErrorDuringExecution,
            "never_ending" => DebugTableType::NeverEnding,
            other => return Err(DebugError::UnknownDebugTableType(other.to_string())),
        })
    }
}

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

impl DebugTableType {
    /// Get the arrow schema for the debug table type.
    pub fn arrow_schema(&self) -> ArrowSchema {
        match self {
            DebugTableType::ErrorDuringExecution => {
                ArrowSchema::new(vec![Field::new("a", DataType::Int32, false)])
            }
            DebugTableType::NeverEnding => ArrowSchema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ]),
        }
    }

    /// Get the projected arrow schema.
    pub fn projected_arrow_schema(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> ArrowResult<ArrowSchema> {
        match projection {
            Some(proj) => self.arrow_schema().project(proj),
            None => Ok(self.arrow_schema()),
        }
    }

    /// Produces a record batch that matches this debug table's schema.
    pub fn record_batch(&self, tunnel: bool) -> RecordBatch {
        let base = if tunnel { 10_i32 } else { 1_i32 };
        match self {
            DebugTableType::ErrorDuringExecution => RecordBatch::try_new(
                Arc::new(self.arrow_schema()),
                vec![Arc::new(Int32Array::from_value(base, 30))],
            )
            .unwrap(),
            DebugTableType::NeverEnding => RecordBatch::try_new(
                Arc::new(self.arrow_schema()),
                vec![
                    Arc::new(Int32Array::from_value(base, 30)),
                    Arc::new(Int32Array::from_value(base * 2, 30)),
                    Arc::new(Int32Array::from_value(base * 3, 30)),
                ],
            )
            .unwrap(),
        }
    }

    /// Get a projected record batch for this debug table type.
    pub fn projected_record_batch(
        &self,
        tunnel: bool,
        projection: Option<&Vec<usize>>,
    ) -> ArrowResult<RecordBatch> {
        match projection {
            Some(proj) => self.record_batch(tunnel).project(proj),
            None => Ok(self.record_batch(tunnel)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DebugTableType::ErrorDuringExecution => "error_during_execution",
            DebugTableType::NeverEnding => "never_ending",
        }
    }

    pub fn into_table_provider(
        self,
        tunnel_opts: Option<&TunnelOptions>,
    ) -> Arc<dyn TableProvider> {
        let tunnel = validate_tunnel_connections(tunnel_opts)
            .expect("datasources should be validated with tunnels before dispatch");
        Arc::new(DebugTableProvider { typ: self, tunnel })
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
    typ: DebugTableType,
    tunnel: bool,
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for DebugTableExec".to_string(),
        ))
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
