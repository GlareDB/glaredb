use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::parquet::{ParquetObjectReader, SimpleParquetFileReaderFactory};

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
        );

        let location = ObjectStorePath::from(access.location);
        let meta = Arc::new(store.head(&location).await?);

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(self, predicate_pushdown: bool) -> Result<S3TableProvider> {
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(S3TableProvider {
            predicate_pushdown,
            accessor: self,
            arrow_schema,
        })
    }
}

pub struct S3TableProvider {
    predicate_pushdown: bool,
    accessor: S3Accessor,
    /// Schema for parquet file
    arrow_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for S3TableProvider {
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
        let predicate = if self.predicate_pushdown {
            filters
                .iter()
                .cloned()
                .reduce(|accum, expr| accum.and(expr))
        } else {
            None
        };

        let file = self.accessor.meta.as_ref().clone().into();

        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(), // to be ignored
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![vec![file]],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: None,
            config_options: ConfigOptions::new().into_shareable(),
        };

        let factory = Arc::new(SimpleParquetFileReaderFactory::new(
            self.accessor.store.clone(),
        ));

        let exec = ParquetExec::new(base_config, predicate, None)
            .with_parquet_file_reader_factory(factory)
            .with_pushdown_filters(self.predicate_pushdown);

        Ok(Arc::new(exec))
    }
}
