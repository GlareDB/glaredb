//! Helpers for handling csv files.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::file_format::{CsvExec as DfCsvExec, FileScanConfig};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::{Expr, SessionContext};

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
        let file = self.accessor.object_meta().as_ref().clone();
        let base_url = self.accessor.location();

        // This config is setup to make use of `FileStream` to stream from csv files in
        // datafusion
        let base_config = FileScanConfig {
            // `store` in `CsvExec` will be used instead of the datafusion object store registry.
            object_store_url: ObjectStoreUrl::parse(&base_url).unwrap(),
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![vec![file.into()]],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: Vec::new(),
            infinite_source: false,
        };
        // Assume csv has a header
        let has_header = true;
        let exec = DfCsvExec::new(
            base_config,
            has_header,
            DEFAULT_DELIMITER,
            DEFAULT_FILE_COMPRESSION_TYPE,
        );
        Ok(Arc::new(exec))
    }
}
const DEFAULT_DELIMITER: u8 = b',';
const DEFAULT_BATCH_SIZE: usize = 8192;
const DEFAULT_FILE_COMPRESSION_TYPE: FileCompressionType = FileCompressionType::UNCOMPRESSED;
