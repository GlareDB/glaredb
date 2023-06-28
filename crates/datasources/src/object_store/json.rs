use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::file_format::{FileScanConfig, FileStream, JsonOpener};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::{Expr, SessionContext};
use object_store::ObjectStore;

use crate::object_store::errors::Result;
use crate::object_store::TableAccessor;

pub struct JsonTableProvider<T>
where
    T: TableAccessor,
{
    pub(crate) accessor: T,
    /// Schema for Json file
    pub(crate) arrow_schema: ArrowSchemaRef,
}

impl<T> JsonTableProvider<T>
where
    T: TableAccessor,
{
    pub async fn from_table_accessor(accessor: T) -> Result<JsonTableProvider<T>> {
        let store = accessor.store();
        let location = [accessor.object_meta().as_ref().clone()];

        // TODO infer schema without generating unused session context/state
        let json_format = JsonFormat::default();
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let arrow_schema = json_format.infer_schema(&state, store, &location).await?;

        Ok(JsonTableProvider {
            accessor,
            arrow_schema,
        })
    }
}

#[async_trait]
impl<T> TableProvider for JsonTableProvider<T>
where
    T: TableAccessor + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let file = self.accessor.object_meta().as_ref().clone().into();

        // This config is setup to make use of `FileStream` to stream from csv files in
        // datafusion
        let base_config = FileScanConfig {
            // `store` in `JsonExec` will be used instead of the datafusion object store registry.
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![vec![file]],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: Vec::new(),
            infinite_source: false,
        };

        // Project the schema.
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        let exec = JsonExec {
            base_config,
            file_compression_type: JsonExec::DEFAULT_FILE_COMPRESSION_TYPE,
            projected_schema,
            store: self.accessor.store().clone(),
        };

        Ok(Arc::new(exec))
    }
}

// /// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
struct JsonExec {
    base_config: FileScanConfig,
    file_compression_type: FileCompressionType,
    projected_schema: ArrowSchemaRef,
    store: Arc<dyn ObjectStore>,
}

impl JsonExec {
    const DEFAULT_BATCH_SIZE: usize = 8192;
    const DEFAULT_FILE_COMPRESSION_TYPE: FileCompressionType = FileCompressionType::UNCOMPRESSED;
}

impl ExecutionPlan for JsonExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> ArrowSchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    /// See comments on `impl ExecutionPlan for ParquetExec`: output order can't
    /// be
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
            "cannot replace children for JsonExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let opener = JsonOpener::new(
            Self::DEFAULT_BATCH_SIZE,
            self.projected_schema.clone(),
            self.file_compression_type.to_owned(),
            self.store.clone(),
        );

        let stream = FileStream::new(
            &self.base_config,
            partition,
            opener,
            &ExecutionPlanMetricsSet::new(),
        )?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let files = self
            .base_config
            .file_groups
            .iter()
            .flatten()
            .map(|f| f.object_meta.location.as_ref())
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "JsonExec: files={files}")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
