//! Helpers for handling csv files.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{CsvExec as DfCsvExec, FileScanConfig};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use object_store::{ObjectMeta, ObjectStore};

use crate::object_store::errors::Result;

use super::FileTypeAccess;

#[derive(Debug)]
pub struct CsvFileAccess {
    pub delimeter: char,
    pub header: bool,
    pub file_compression: FileCompressionType,
}

impl Default for CsvFileAccess {
    fn default() -> Self {
        Self {
            delimeter: ',',
            header: true,
            file_compression: FileCompressionType::UNCOMPRESSED,
        }
    }
}

#[async_trait]
impl FileTypeAccess for CsvFileAccess {
    async fn get_schema(
        &self,
        store: Arc<dyn object_store::ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<ArrowSchemaRef> {
        let csv_format = CsvFormat::default();
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let arrow_schema = csv_format.infer_schema(&state, &store, objects).await?;
        Ok(arrow_schema)
    }

    async fn get_exec_plan(
        &self,
        _ctx: &SessionState,
        _store: Arc<dyn ObjectStore>,
        conf: FileScanConfig,
        _filters: &[Expr],
        _predicate_pushdown: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = DfCsvExec::new(
            conf,
            self.header,
            self.delimeter as u8,
            self.file_compression.clone(),
        );
        Ok(Arc::new(exec))
    }
}
