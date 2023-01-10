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
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::parquet::{ParquetExec, ParquetObjectReader};

/// Information needed for accessing an external Parquet file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3TableAccess {
    /// S3 object store region
    pub region: String,
    /// S3 object store bucket name
    pub bucket_name: String,
    /// S3 object store access key id
    pub access_key_id: String,
    /// S3 object store secret access key
    pub secret_access_key: String,
    /// S3 object store table location
    pub location: String,
}

#[derive(Debug)]
pub struct S3Accessor {
    /// S3 object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
}

impl S3Accessor {
    /// Setup accessor for S3
    pub async fn new(access: S3TableAccess) -> Result<Self> {
        let store = Arc::new(
            AmazonS3Builder::new()
                .with_region(access.region)
                .with_bucket_name(access.bucket_name)
                .with_access_key_id(access.access_key_id)
                .with_secret_access_key(access.secret_access_key)
                .build()?,
        ) as Arc<dyn ObjectStore>;

        let location = ObjectStorePath::from(access.location);
        let meta = Arc::new(store.head(&location).await?);

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(
        self,
        _predicate_pushdown: bool,
    ) -> Result<ParquetTableProvider> {
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(ParquetTableProvider {
            _predicate_pushdown,
            accessor: self,
            arrow_schema,
        })
    }
}

pub struct ParquetTableProvider {
    _predicate_pushdown: bool,
    accessor: S3Accessor,
    arrow_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for ParquetTableProvider {
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
