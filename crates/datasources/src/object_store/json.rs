use std::any::Any;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, NdJsonExec};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::{Expr, SessionContext};

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
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        self.accessor.validate_access(ctx)?;

        let file = self.accessor.object_meta().as_ref().clone().into();
        let base_url = self.accessor.base_path();

        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse(base_url)
                .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem()),
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
        let _projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        let exec = NdJsonExec::new(base_config, FileCompressionType::UNCOMPRESSED);

        Ok(Arc::new(exec))
    }
}
