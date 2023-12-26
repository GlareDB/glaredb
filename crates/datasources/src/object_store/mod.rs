use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::FileType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::{get_statistics_with_limit, TableProvider};
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_ext::metrics::ReadOnlyDataSourceMetricsExecAdapter;
use errors::ObjectStoreSourceError;
use errors::Result;
use futures::StreamExt;
use glob::{MatchOptions, Pattern};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use protogen::metastore::types::options::{TableOptions, TableOptionsObjectStore};

use crate::common::exprs_to_phys_exprs;
use crate::common::url::DatasourceUrl;
use crate::object_store::gcs::GcsStoreAccess;
use crate::object_store::generic::GenericStoreAccess;
use crate::object_store::local::LocalStoreAccess;
use crate::object_store::s3::S3StoreAccess;

pub mod errors;
pub mod gcs;
pub mod generic;
pub mod http;
pub mod local;
pub mod s3;

pub struct MultiSourceTableProvider {
    sources: Vec<Arc<dyn TableProvider>>,
}
impl MultiSourceTableProvider {
    pub fn new<T: IntoIterator<Item = Arc<dyn TableProvider>>>(sources: T) -> Self {
        Self {
            sources: sources.into_iter().collect(),
        }
    }
}

#[async_trait]
impl TableProvider for MultiSourceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.sources.first().unwrap().schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if self.sources.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "no sources found".to_string(),
            ));
        }
        if self
            .sources
            .as_slice()
            .windows(2)
            .any(|w| w[0].schema() != w[1].schema())
        {
            return Err(datafusion::error::DataFusionError::Execution(
                "schemas of sources do not match".to_string(),
            ));
        }
        let mut plans = Vec::new();
        for source in &self.sources {
            let plan = source
                .scan(state, projection, filters, limit)
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            plans.push(plan);
        }

        if plans.len() == 1 {
            Ok(plans.pop().unwrap())
        } else {
            Ok(Arc::new(UnionExec::new(plans)))
        }
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

    /// Gets a list of objects that match the glob pattern.
    async fn list_globbed(
        &self,
        store: &Arc<dyn ObjectStore>,
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
        store: &Arc<dyn ObjectStore>,
        location: &ObjectStorePath,
    ) -> Result<ObjectMeta> {
        Ok(store.head(location).await?)
    }

    async fn create_table_provider(
        &self,
        state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        locations: Vec<DatasourceUrl>,
    ) -> Result<Arc<dyn TableProvider>> {
        let store = self.create_store()?;
        let mut objects = Vec::new();
        for loc in locations {
            let list = self
                .list_globbed(&store, &loc.path())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if list.is_empty() {
                let e = object_store::path::Error::InvalidPath {
                    path: loc.to_string().into(),
                };
                return Err(ObjectStoreSourceError::ObjectStorePath(e));
            }

            objects.push(list);
        }
        let objects = objects.into_iter().flatten().collect::<Vec<_>>();

        let arrow_schema = file_format.infer_schema(state, &store, &objects).await?;
        let base_url = self.base_url()?;

        Ok(Arc::new(ObjStoreTableProvider {
            store,
            arrow_schema,
            base_url,
            objects,
            file_format,
        }))
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
            .list_globbed(&self.store, pattern.as_ref())
            .await
    }

    /// Takes all the objects and creates the table provider from the accesor.
    pub async fn into_table_provider(
        self,
        state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        objects: Vec<ObjectMeta>,
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
        }))
    }
}

#[derive(Debug)]
pub struct ObjStoreTableProvider {
    store: Arc<dyn ObjectStore>,
    arrow_schema: SchemaRef,
    base_url: ObjectStoreUrl,
    objects: Vec<ObjectMeta>,
    file_format: Arc<dyn FileFormat>,
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
        // See datafusion's `ListingTable::list_files_for_scan`.
        let files = futures::stream::iter(&self.objects)
            .map(|object| async {
                let file: PartitionedFile = object.clone().into();
                let stats = self
                    .file_format
                    .infer_stats(ctx, &self.store, self.schema(), object)
                    .await?;
                Ok((file, stats))
            })
            .boxed()
            .buffered(ctx.config_options().execution.meta_fetch_concurrency);
        let (files, statistics) = get_statistics_with_limit(files, self.schema(), limit).await?;

        let config = FileScanConfig {
            object_store_url: self.base_url.clone(),
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![files],
            statistics,
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

        let plan = self
            .file_format
            .create_physical_plan(ctx, config, filters.as_ref())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(ReadOnlyDataSourceMetricsExecAdapter::new(plan)))
    }
}

pub fn file_type_from_path(path: &ObjectStorePath) -> Result<FileType> {
    path.extension()
        .ok_or(ObjectStoreSourceError::NoFileExtension)?
        .parse()
        .map_err(ObjectStoreSourceError::DataFusion)
}

pub fn init_session_registry<'a>(
    runtime: &RuntimeEnv,
    entries: impl Iterator<Item = &'a TableOptions>,
) -> Result<()> {
    for opts in entries {
        let access: Arc<dyn ObjStoreAccess> = match opts {
            TableOptions::Local(_) => Arc::new(LocalStoreAccess),
            // TODO: Consider consolidating Gcs, S3 and Delta and Iceberg `TableOptions` and
            // `ObjStoreAccess` since they largely overlap
            TableOptions::Gcs(opts) => Arc::new(GcsStoreAccess {
                bucket: opts.bucket.clone(),
                service_account_key: opts.service_account_key.clone(),
            }),
            TableOptions::S3(opts) => Arc::new(S3StoreAccess {
                region: opts.region.clone(),
                bucket: opts.bucket.clone(),
                access_key_id: opts.access_key_id.clone(),
                secret_access_key: opts.secret_access_key.clone(),
            }),
            TableOptions::Delta(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            })
            | TableOptions::Iceberg(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            })
            | TableOptions::Azure(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            })
            | TableOptions::Lance(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            })
            | TableOptions::Bson(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            }) => Arc::new(GenericStoreAccess::new_from_location_and_opts(
                location,
                storage_options.clone(),
            )?),
            // Continue on all others. Explicitly mentioning all the left
            // over options so we don't forget adding object stores that are
            // supported in the future (like azure).
            TableOptions::Internal(_)
            | TableOptions::Debug(_)
            | TableOptions::Postgres(_)
            | TableOptions::BigQuery(_)
            | TableOptions::Mysql(_)
            | TableOptions::Mongo(_)
            | TableOptions::Snowflake(_)
            | TableOptions::SqlServer(_) => continue,
        };

        let base_url = access.base_url()?;
        let store = access.create_store()?;
        runtime.register_object_store(base_url.as_ref(), store);
    }

    Ok(())
}
