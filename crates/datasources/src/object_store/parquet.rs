//! Helpers for handling parquet files.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::ToDFSchema;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::execution::context::SessionState;
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use object_store::{ObjectMeta, ObjectStore};

use super::errors::Result;
use super::FileTypeAccess;

#[derive(Debug, Default)]
pub struct ParquetFileAccess;

#[async_trait]
impl FileTypeAccess for ParquetFileAccess {
    async fn get_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<ArrowSchemaRef> {
        let parquet_format = ParquetFormat::default();
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let arrow_schema = parquet_format.infer_schema(&state, &store, objects).await?;
        Ok(arrow_schema)
    }

    async fn get_exec_plan(
        &self,
        ctx: &SessionState,
        _store: Arc<dyn ObjectStore>,
        conf: FileScanConfig,
        filters: &[Expr],
        predicate_pushdown: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let predicate = if predicate_pushdown {
            // Adapted from df...
            if let Some(expr) = conjunction(filters.to_vec()) {
                // NOTE: Use the table schema (NOT file schema) here because
                // `expr` may contain references to partition columns.
                let table_df_schema = conf.file_schema.as_ref().clone().to_dfschema()?;
                let filter = create_physical_expr(
                    &expr,
                    &table_df_schema,
                    &conf.file_schema,
                    ctx.execution_props(),
                )?;

                Some(filter)
            } else {
                None
            }
        } else {
            None
        };

        let exec =
            ParquetExec::new(conf, predicate, None).with_pushdown_filters(predicate_pushdown);
        Ok(Arc::new(exec))
    }
}
