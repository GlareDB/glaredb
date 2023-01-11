use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_plan::ExecutionPlan;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::errors::Result;
use crate::parquet::{ParquetExec, ParquetObjectReader};

/// Information needed for accessing an Parquet file on local file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTableAccess {
    pub location: String,
}

#[derive(Debug)]
pub struct LocalAccessor {
    /// Local filesystem  object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
}

impl LocalAccessor {
    /// Setup accessor for Local file system
    pub async fn new(access: LocalTableAccess) -> Result<Self> {
        let store = Arc::new(LocalFileSystem::new());

        let location = ObjectStorePath::from(access.location);
        trace!(?location, "location");
        let meta = Arc::new(store.head(&location).await?);

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(
        self,
        _predicate_pushdown: bool,
    ) -> Result<LocalTableProvider> {
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(LocalTableProvider {
            _predicate_pushdown,
            accessor: self,
            arrow_schema,
        })
    }
}

pub struct LocalTableProvider {
    _predicate_pushdown: bool,
    accessor: LocalAccessor,
    /// Schema for parquet file
    arrow_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for LocalTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.arrow_schema.project(projection)?),
            None => self.arrow_schema.clone(),
        };

        let exec = ParquetExec {
            store: self.accessor.store.clone(),
            arrow_schema: projected_schema,
            meta: self.accessor.meta.clone(),
            projection: projection.cloned(),
        };

        Ok(Arc::new(exec))
    }
}
