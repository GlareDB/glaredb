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

use crate::csv::csv_helper::{CsvConfig, CsvOpener};
use crate::errors::Result;
use crate::TableAccessor;

/// Table provider for csv table
pub struct CsvTableProvider<T>
where
    T: TableAccessor,
{
    pub(crate) predicate_pushdown: bool,
    pub(crate) accessor: T,
    /// Schema for csv file
    pub(crate) arrow_schema: ArrowSchemaRef,
}

impl<T> CsvTableProvider<T>
where
    T: TableAccessor,
{
    pub async fn from_table_accessor(
        accessor: T,
        predicate_pushdown: bool,
    ) -> Result<CsvTableProvider<T>> {
        let store = accessor.store();
        let location = [accessor.object_meta().as_ref().clone()];

        let csv_format = CsvFormat::default();
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        // TODO get schema another way
        let arrow_schema = csv_format.infer_schema(&state, store, &location).await?;

        Ok(CsvTableProvider {
            predicate_pushdown,
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let _predicate = if self.predicate_pushdown {
            filters
                .iter()
                .cloned()
                .reduce(|accum, expr| accum.and(expr))
        } else {
            None
        };

        let file = self.accessor.object_meta().as_ref().clone().into();

        let base_config = FileScanConfig {
            // `store` to `CsvExec` to use instead of the datafusion object store registry.
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![vec![file]],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: None,
            infinite_source: false,
        };

        let has_header = true;
        let delimiter = b',';

        // Project the schema.
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        let exec = CsvExec {
            base_config,
            has_header,
            delimiter,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
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
            batch_size: 8192,
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
        // TODO: need proper display for CsvExec
        tracing::warn!("No display format for CsvExec");
        write!(f, "CsvExec: file=")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
