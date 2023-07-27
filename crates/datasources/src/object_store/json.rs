use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileScanConfig, NdJsonExec};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use object_store::{ObjectMeta, ObjectStore};

use crate::object_store::errors::Result;

use super::FileTypeAccess;

#[derive(Debug, Default)]
pub struct JsonFileAccess;

#[async_trait]
impl FileTypeAccess for JsonFileAccess {
    async fn get_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<ArrowSchemaRef> {
        // TODO infer schema without generating unused session context/state
        let json_format = JsonFormat::default();
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let arrow_schema = json_format.infer_schema(&state, &store, objects).await?;
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
        let exec = NdJsonExec::new(conf, FileCompressionType::UNCOMPRESSED);
        Ok(Arc::new(exec))
    }
}
