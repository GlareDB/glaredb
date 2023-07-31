use std::any::Any;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use errors::ObjectStoreSourceError;
use futures::StreamExt;
use glob::{MatchOptions, Pattern};
use metastore_client::types::options::TableOptions;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use errors::Result;

use crate::object_store::gcs::GcsStoreAccess;
use crate::object_store::local::LocalStoreAccess;
use crate::object_store::s3::S3StoreAccess;

pub mod csv;
pub mod errors;
pub mod gcs;
pub mod http;
pub mod json;
pub mod local;
pub mod parquet;
pub mod s3;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FileType {
    Csv,
    Parquet,
    Json,
}

impl FromStr for FileType {
    type Err = ObjectStoreSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let typ = if s.eq_ignore_ascii_case("parquet") {
            Self::Parquet
        } else if s.eq_ignore_ascii_case("csv") {
            Self::Csv
        } else if s.eq_ignore_ascii_case("json") {
            Self::Json
        } else {
            return Err(Self::Err::NotSupportFileType(s.to_owned()));
        };
        Ok(typ)
    }
}

#[async_trait]
pub trait FileTypeAccess: Debug + Send + Sync {
    /// Gets the schema using objects.
    async fn get_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef>;

    /// Transform into an execution plan.
    async fn get_exec_plan(
        &self,
        ctx: &SessionState,
        store: Arc<dyn ObjectStore>,
        conf: FileScanConfig,
        filters: &[Expr],
        predicate_pushdown: bool,
    ) -> Result<Arc<dyn ExecutionPlan>>;
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
        store: Arc<dyn ObjectStore>,
        pattern: &str,
    ) -> Result<Vec<ObjectMeta>> {
        if let Some((prefix, _)) = pattern.split_once(['*', '?', '!', '[', ']']) {
            // This pattern might actually be a "glob" pattern.
            //
            // NOTE: Break the path at "/" (delimeter) since `object_store` will
            // add it and assume that's the prefix. Yes, it's annoying but what
            // can you do :shrug:!
            let prefix =
                if let Some((new_prefix, _)) = prefix.rsplit_once(object_store::path::DELIMITER) {
                    new_prefix
                } else {
                    prefix
                };

            let objects = {
                let prefix = self.path(prefix)?;
                let mut object_futs = store.list(Some(&prefix)).await?;

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

    /// Returns a list of objects from locations.
    pub async fn list<S, V>(&self, locations: V) -> Result<Vec<ObjectMeta>>
    where
        S: AsRef<str>,
        V: Iterator<Item = S>,
    {
        let mut objects = Vec::new();
        for location in locations {
            let path = self.access.path(location.as_ref())?;
            let meta = self.access.object_meta(self.store.clone(), &path).await?;
            objects.push(meta);
        }
        Ok(objects)
    }

    pub async fn into_table_provider(
        self,
        file_access: Arc<dyn FileTypeAccess>,
        objects: Vec<ObjectMeta>,
        predicate_pushdown: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        let store = self.store;
        let arrow_schema = file_access.get_schema(store.clone(), &objects).await?;
        let base_url = self.access.base_url()?;

        Ok(Arc::new(ObjStoreTableProvider {
            store,
            arrow_schema,
            base_url,
            objects,
            file_access,
            predicate_pushdown,
        }))
    }
}

pub struct ObjStoreTableProvider {
    store: Arc<dyn ObjectStore>,
    arrow_schema: SchemaRef,
    base_url: ObjectStoreUrl,
    objects: Vec<ObjectMeta>,
    file_access: Arc<dyn FileTypeAccess>,
    predicate_pushdown: bool,
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

        // We register the store at scan time so that it can be used by the
        // exec plan.
        ctx.runtime_env()
            .register_object_store(self.base_url.as_ref(), self.store.clone());

        self.file_access
            .get_exec_plan(
                ctx,
                self.store.clone(),
                config,
                filters,
                self.predicate_pushdown,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

pub fn file_type_from_path(path: &ObjectStorePath) -> Result<FileType> {
    path.extension()
        .ok_or(ObjectStoreSourceError::NoFileExtension)?
        .parse()
}

pub fn init_session_registry<'a>(
    runtime: &RuntimeEnv,
    entries: impl Iterator<Item = &'a TableOptions>,
) -> Result<()> {
    for opts in entries {
        let access: Arc<dyn ObjStoreAccess> = match opts {
            TableOptions::Local(_) => Arc::new(LocalStoreAccess),
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
            // Continue on all others. Explicityly mentioning all the left
            // over options so we don't forget adding object stores that are
            // supported in the furure (like azure).
            TableOptions::Internal(_)
            | TableOptions::Debug(_)
            | TableOptions::Postgres(_)
            | TableOptions::BigQuery(_)
            | TableOptions::Mysql(_)
            | TableOptions::Mongo(_)
            | TableOptions::Snowflake(_) => continue,
        };

        let base_url = access.base_url()?;
        let store = access.create_store()?;
        runtime.register_object_store(base_url.as_ref(), store);
    }

    Ok(())
}
