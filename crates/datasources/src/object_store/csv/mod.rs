//! Helpers for handling csv files.

mod csv_helper;

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::file_format::{FileScanConfig, FileStream};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::{Expr, SessionContext};
use object_store::ObjectStore;

use crate::object_store::csv::csv_helper::{CsvConfig, CsvOpener};
use crate::object_store::errors::Result;
use crate::object_store::TableAccessor;

/// Table provider for csv table
pub struct CsvTableProvider<T>
where
    T: TableAccessor,
{
    pub(crate) accessor: T,
    /// Schema for csv file
    pub(crate) arrow_schema: ArrowSchemaRef,
}

impl<T> CsvTableProvider<T>
where
    T: TableAccessor,
{
    pub async fn from_table_accessor(accessor: T) -> Result<CsvTableProvider<T>> {
        let store = accessor.store();
        let location = [accessor.object_meta().as_ref().clone()];

        // TODO infer schema without generating unused session context/state
        let csv_format = CsvFormat::default();
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let arrow_schema = csv_format.infer_schema(&state, store, &location).await?;

        Ok(CsvTableProvider {
            accessor,
            arrow_schema,
        })
    }
}

#[async_trait]
impl<T> TableProvider for CsvTableProvider<T>
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
            // `store` in `CsvExec` will be used instead of the datafusion object store registry.
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

        // Assume csv has a header
        let has_header = true;

        // Project the schema.
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        let exec = CsvExec {
            base_config,
            has_header,
            delimiter: CsvExec::DEFAULT_DELIMITER,
            file_compression_type: CsvExec::DEFAULT_FILE_COMPRESSION_TYPE,
            arrow_schema: self.arrow_schema.clone(),
            projection: projection.cloned(),
            projected_schema,
            store: self.accessor.store().clone(),
        };

        Ok(Arc::new(exec))
    }
}

// /// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
struct CsvExec {
    base_config: FileScanConfig,
    delimiter: u8,
    has_header: bool,
    file_compression_type: FileCompressionType,
    arrow_schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: ArrowSchemaRef,
    store: Arc<dyn ObjectStore>,
}

impl CsvExec {
    const DEFAULT_DELIMITER: u8 = b',';
    const DEFAULT_BATCH_SIZE: usize = 8192;
    const DEFAULT_FILE_COMPRESSION_TYPE: FileCompressionType = FileCompressionType::UNCOMPRESSED;
}

impl ExecutionPlan for CsvExec {
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
            "cannot replace children for CsvExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let config = CsvConfig {
            batch_size: Self::DEFAULT_BATCH_SIZE,
            file_schema: self.arrow_schema.clone(),
            file_projection: self.projection.clone(),
            has_header: self.has_header,
            delimiter: self.delimiter,
            object_store: self.store.clone(),
        };

        let opener = CsvOpener {
            config,
            file_compression_type: self.file_compression_type.to_owned(),
        };

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

        write!(f, "CsvExec: files={files}")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
