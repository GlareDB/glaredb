use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::csv::{CsvFormat, DEFAULT_CSV_EXTENSION};
use datafusion::datasource::file_format::json::{JsonFormat, DEFAULT_JSON_EXTENSION};
use datafusion::datasource::file_format::parquet::{ParquetFormat, DEFAULT_PARQUET_EXTENSION};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use errors::ObjectStoreSourceError;
use futures::{StreamExt, TryStreamExt};
use glob::{MatchOptions, Pattern};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};

use datafusion::datasource::file_format::file_type::FileType;
use errors::Result;

use crate::common::exprs_to_phys_exprs;

pub mod errors;
pub mod gcs;
pub mod http;
pub mod local;
pub mod registry;
pub mod s3;

pub fn filetype_as_file_format_and_ext(ft: &FileType) -> (Arc<dyn FileFormat>, &'static str) {
    match ft {
        FileType::PARQUET => (
            Arc::new(ParquetFormat::default()),
            DEFAULT_PARQUET_EXTENSION,
        ),
        FileType::CSV => (Arc::new(CsvFormat::default()), DEFAULT_CSV_EXTENSION),
        FileType::JSON => (Arc::new(JsonFormat::default()), DEFAULT_JSON_EXTENSION),
        _ => todo!(),
    }
}

#[async_trait]
pub trait ObjStoreAccess: Debug + Display + Send + Sync {
    /// Returns the base URL against which the object store is registered.
    ///
    /// Examples:
    /// * `s3//bucket/path/to/file.csv`: `s3://bucket`
    /// * `/some/local/file`: `file://`
    /// * `https://abc.com/xyz/pqr`: `https://abc.com__slash__xyz__slash__pqr`
    ///
    fn base_url(&self) -> Result<ObjectStoreUrl>;

    /// Creates an object store.
    fn create_store(&self) -> Result<Arc<dyn ObjectStore>>;

    /// Gets the object store path.
    fn path(&self, location: &str) -> Result<ObjectStorePath>;

    async fn list_all_files(
        &self,
        listing_url: &ListingTableUrl,
        store: &Arc<dyn ObjectStore>,
        file_extension: &str,
    ) -> Result<Vec<ObjectMeta>>
    where
        Self: Sized,
    {
        // If the prefix is a file, use a head request, otherwise list
        let pattern = listing_url.as_str();

        let is_dir = pattern.ends_with('/');
        let is_glob = pattern.contains(['*', '?', '[', ']', '!']);

        if is_dir || is_glob {
            let list = futures::stream::once(store.list(Some(listing_url.prefix())))
                .try_flatten()
                .boxed();
            let list = list
                .map_err(ObjectStoreSourceError::ObjectStore)
                .try_filter(move |meta| {
                    let path = &meta.location;
                    let extension_match = path.as_ref().ends_with(file_extension);
                    let glob_match = listing_url.contains(path);
                    println!(
                        "path: {}, extension_match: {}, glob_match: {}",
                        path, extension_match, glob_match
                    );
                    futures::future::ready(extension_match && glob_match)
                })
                .boxed();
            list.try_collect().await
        } else {
            let meta = store.head(listing_url.prefix()).await?;
            Ok(vec![meta])
        }
    }

    /// Gets a list of objects that match the glob pattern.
    async fn list_globbed(
        &self,
        store: Arc<dyn ObjectStore>,
        pattern: &str,
    ) -> Result<Vec<ObjectMeta>> {
        if let Some((prefix, _)) = pattern.split_once(['*', '?', '!', '[', ']']) {
            // This pattern might actually be a "glob" pattern.
            //
            // NOTE: Break the path at "/" (delimeter) since `object_store` will
            // add it and assume that's the prefix. Yes, it's annoying but what
            // can you do :shrug:!
            let prefix = prefix
                .rsplit_once(object_store::path::DELIMITER)
                .map(|(new_prefix, _)| self.path(new_prefix))
                .transpose()?;

            let objects = {
                let mut object_futs = store.list(prefix.as_ref()).await?;

                let pattern = Pattern::new(pattern)?;
                const MATCH_OPTS: MatchOptions = MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                };

                let mut objects = Vec::new();
                while let Some(object) = object_futs.next().await {
                    let object = object?;
                    if pattern.matches_with(object.location.as_ref(), MATCH_OPTS) {
                        objects.push(object);
                    }
                }
                objects
            };

            Ok(objects)
        } else {
            // Definitely not a "glob" pattern.
            let location = self.path(pattern)?;
            let meta = store.head(&location).await?;
            Ok(vec![meta])
        }
    }

    /// Returns the object meta given location of the object.
    async fn object_meta(
        &self,
        store: Arc<dyn ObjectStore>,
        location: &ObjectStorePath,
    ) -> Result<ObjectMeta> {
        Ok(store.head(location).await?)
    }
}

#[derive(Debug, Clone)]
pub struct ObjStoreAccessor {
    store: Arc<dyn ObjectStore>,
    access: Arc<dyn ObjStoreAccess>,
}

impl Display for ObjStoreAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.access)
    }
}

impl ObjStoreAccessor {
    /// Creates a new object store accessor for the given access.
    pub fn new(access: Arc<dyn ObjStoreAccess>) -> Result<Self> {
        Ok(Self {
            store: access.create_store()?,
            access,
        })
    }

    /// Returns a list of objects matching the globbed pattern.
    pub async fn list_globbed(&self, pattern: impl AsRef<str>) -> Result<Vec<ObjectMeta>> {
        self.access
            .list_globbed(self.store.clone(), pattern.as_ref())
            .await
    }

    /// Takes all the objects and creates the table provider from the accesor.
    pub async fn into_table_provider(
        self,
        state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        objects: Vec<ObjectMeta>,
        predicate_pushdown: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        let store = self.store;
        let arrow_schema = file_format.infer_schema(state, &store, &objects).await?;
        let base_url = self.access.base_url()?;

        Ok(Arc::new(ObjStoreTableProvider {
            store,
            arrow_schema,
            base_url,
            objects,
            file_format,
            _predicate_pushdown: predicate_pushdown,
        }))
    }
}

pub struct ObjStoreTableProvider {
    store: Arc<dyn ObjectStore>,
    arrow_schema: SchemaRef,
    base_url: ObjectStoreUrl,
    objects: Vec<ObjectMeta>,
    file_format: Arc<dyn FileFormat>,
    _predicate_pushdown: bool,
}

#[async_trait]
impl TableProvider for ObjStoreTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
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
        let config = FileScanConfig {
            object_store_url: self.base_url.clone(),
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![self.objects.iter().map(|o| o.clone().into()).collect()],
            statistics: Default::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: Vec::new(),
            infinite_source: false,
        };

        let filters = exprs_to_phys_exprs(filters, ctx, &self.arrow_schema)?;

        // We register the store at scan time so that it can be used by the
        // exec plan.
        ctx.runtime_env()
            .register_object_store(self.base_url.as_ref(), self.store.clone());

        self.file_format
            .create_physical_plan(ctx, config, filters.as_ref())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

pub fn file_type_from_path(path: &ObjectStorePath) -> Result<FileType> {
    path.extension()
        .ok_or(ObjectStoreSourceError::NoFileExtension)?
        .parse()
        .map_err(ObjectStoreSourceError::DataFusion)
}
