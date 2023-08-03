use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

use crate::object_store::errors::ObjectStoreSourceError;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::file_type::FileType;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use url::Url;

use super::errors::Result;
use super::filetype_as_file_format_and_ext;
use super::ObjStoreAccess;
use datafusion::error::{DataFusionError, Result as DatafusionResult};

#[derive(Debug)]
pub struct GcsProvider {
    pub config: ListingTableConfig,
    service_account_key_json: Option<String>,
}

impl Display for GcsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GCS(bucket: )")
    }
}

impl GcsProvider {
    pub fn new(config: ListingTableConfig, service_account_key_json: Option<String>) -> Self {
        Self {
            config,
            service_account_key_json,
        }
    }

    pub fn store(&self) -> Result<Arc<dyn ObjectStore>> {
        // `table_paths` are already checked before this point.
        let bucket_name: &Url = self.config.table_paths.get(0).unwrap().as_ref();
        let bucket_name =
            bucket_name
                .host_str()
                .map(|b| b.to_owned())
                .ok_or(ObjectStoreSourceError::Static(
                    "expected bucket name in URL",
                ))?;
        let builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket_name);
        let store = match &self.service_account_key_json {
            Some(key) => builder.with_service_account_key(key),
            None => {
                // TODO: Null Credentials
                builder
            }
        }
        .build()?;

        Ok(Arc::new(store))
    }

    pub async fn infer(&self, ctx: &SessionState, filetype: &FileType) -> Result<Self> {
        let mut config = self.config.clone();
        let table_paths = config.clone().table_paths;
        let listing_url = table_paths.get(0).unwrap();

        let (file_fmt, file_ext) = filetype_as_file_format_and_ext(filetype);

        let listing_options = ListingOptions::new(file_fmt).with_file_extension(file_ext);

        // create a store with valid credentials,
        // then let ListingTable handle the rest.
        let store = self
            .store()
            .map_err(|e| DataFusionError::External(e.into()))?;
        ctx.runtime_env()
            .register_object_store(listing_url.as_ref(), store.clone());
        ctx.runtime_env()
            .register_object_store(listing_url.object_store().as_ref(), store.clone());

        let files = self.list_all_files(listing_url, &store, file_ext).await?;

        config = config.with_listing_options(listing_options);

        let schema = config
            .options
            .as_ref()
            .unwrap()
            .format
            .infer_schema(ctx, &store, &files)
            .await?;

        config = config.with_schema(schema);

        Ok(Self {
            config,
            service_account_key_json: self.service_account_key_json.clone(),
        })
    }
}

#[async_trait]
impl TableProvider for GcsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.config.clone().file_schema.unwrap()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let prov = ListingTable::try_new(self.config.clone())?;
        prov.scan(ctx, projection, filters, limit).await
    }
}

/// Information needed for accessing an external Parquet file on Google Cloud
/// Storage.

#[derive(Debug, Clone)]
pub struct GcsStoreAccess {
    /// Bucket name for GCS store.
    pub bucket: String,
    /// Service account key (JSON) for credentials.
    pub service_account_key: Option<String>,
}

impl Display for GcsStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GCS(bucket: {})", self.bucket)
    }
}

impl ObjStoreAccess for GcsStoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        let u = format!("gs://{}", self.bucket);
        let u = ObjectStoreUrl::parse(u)?;
        Ok(u)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let builder = GoogleCloudStorageBuilder::new().with_bucket_name(&self.bucket);
        let builder = match &self.service_account_key {
            Some(key) => builder.with_service_account_key(key),
            None => {
                // TODO: Null Credentials
                builder
            }
        };
        let build = builder.build()?;
        Ok(Arc::new(build))
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}

impl ObjStoreAccess for GcsProvider {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        let url = &self.config.table_paths.get(0).unwrap().object_store();
        Ok(url.clone())
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        self.store()
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        println!("PATH CALLED");
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}
