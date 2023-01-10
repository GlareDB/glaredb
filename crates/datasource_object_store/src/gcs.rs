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
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::parquet::{ParquetExec, ParquetObjectReader};

/// Information needed for accessing an external Parquet file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsTableAccess {
    /// GCS object store bucket name
    pub bucket_name: String,
    /// GCS object store service account key
    pub service_acccount_key_json: String,
    /// GCS object store table location
    pub location: String,
}

#[derive(Debug)]
pub struct GcsAccessor {
    /// GCS object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
}

impl GcsAccessor {
    /// Setup accessor for GCS
    pub async fn new(access: GcsTableAccess) -> Result<Self> {
        let store = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_service_account_key(access.service_acccount_key_json)
                .with_bucket_name(access.bucket_name)
                .build()?,
        ) as Arc<dyn ObjectStore>;

        let location = ObjectStorePath::from(access.location);
        let meta = Arc::new(store.head(&location).await?);

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(self, _predicate_pushdown: bool) -> Result<GcsTableProvider> {
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(GcsTableProvider {
            _predicate_pushdown,
            accessor: self,
            arrow_schema,
        })
    }
}

pub struct GcsTableProvider {
    _predicate_pushdown: bool,
    accessor: GcsAccessor,
    arrow_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for GcsTableProvider {
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
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // Projection.
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

        let exec = Arc::new(exec) as Arc<dyn ExecutionPlan>;
        Ok(exec)
    }
}
