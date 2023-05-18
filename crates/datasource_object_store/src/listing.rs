//! Module for adapting the `ListingTable` from DataFusion.
//!
//! Note that there is a tiny bit of jank associated with this to support
//! multiple object stores for our use case. DataFusion supports unique object
//! stores keyed by scheme+bucket, but this isn't enough for us since we support
//! multiple object stores with different sets of credentials.
use crate::errors::{ObjectStoreSourceError, Result};
use crate::{FileCompressionType, FileType};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::{
    avro::AvroFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat,
};
use datafusion::datasource::listing::{ListingOptions, ListingTable};
use datafusion::datasource::listing::{ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Statistics;
use datafusion::prelude::Expr;
use object_store::ObjectStore;
use parking_lot::Mutex;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use tracing::trace;
use url::Url;

/// Query param we look for on the URL.
pub const HASH_QUERY_PARAM: &str = "glaredb_hash";

/// Used in conjunction with the listing table and custom object store registry.
pub trait ObjectStoreHasher {
    /// Generate a unique URL for this object store.
    fn generate_url(&self) -> url::Url;

    /// Get the object store.
    fn build_object_store(&self) -> Result<Arc<dyn ObjectStore>>;
}

pub(crate) fn build_url_with_hash(scheme: &str, host: &str, hash: impl Hash) -> url::Url {
    let mut hasher = DefaultHasher::new();
    hash.hash(&mut hasher);
    let hash = hasher.finish();

    url::Url::parse(&format!("{scheme}://{host}/?{HASH_QUERY_PARAM}={hash}")).unwrap()
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct StoreKey {
    scheme: String,
    hash: String,
}

impl StoreKey {
    fn from_url(url: &url::Url) -> Self {
        let hash = match url.query_pairs().find(|(k, _v)| k == HASH_QUERY_PARAM) {
            Some((_, v)) => v.to_string(),
            None => String::new(),
        };

        StoreKey {
            scheme: url.scheme().to_string(),
            hash,
        }
    }
}

/// An object store registry that keys object stores by credential hashes.
///
/// This is needed because while there may exist multiple external tables that
/// reference the same object store, each may also use a different set of
/// credentials. DataFusion's default registry does not let us make this
/// distinction.
///
/// To make this distinction, we (ab)use URLs with a query param that that
/// represents the hashed credentials, with this hash corresponding to a unique
/// object store.
#[derive(Debug, Default)]
pub struct HashedObjectStoreRegistry {
    stores: Mutex<HashMap<StoreKey, Arc<dyn ObjectStore>>>,
}

impl ObjectStoreRegistry for HashedObjectStoreRegistry {
    fn register_store(
        &self,
        url: &url::Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let key = StoreKey::from_url(url);
        let mut stores = self.stores.lock();
        stores.insert(key, store).clone()
    }

    fn get_store(&self, url: &url::Url) -> DatafusionResult<Arc<dyn ObjectStore>> {
        let key = StoreKey::from_url(url);
        let stores = self.stores.lock();
        let store = stores
            .get(&key)
            .ok_or_else(|| {
                DataFusionError::Internal(format!("No suitable object store found for {url}"))
            })?
            .clone();

        Ok(store)
    }
}

/// Creates a table provider for listing from multiple partitions/files in
/// object storage.
///
/// Based off of DataFusion's `ListingTableFactory`.
#[derive(Debug, Clone)]
pub struct ListingTableCreator;

impl ListingTableCreator {
    pub fn new(
        state: &SessionState,
        store: impl ObjectStoreHasher,
        file_type: Option<FileType>,
        location: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let ext = get_extension(location);
        let file_type = match file_type {
            Some(ft) => ft,
            None => file_type_from_extension(location)?,
        };

        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::Parquet => Arc::new(ParquetFormat::new()),
            FileType::Csv => Arc::new(CsvFormat::default()),
            _ => unimplemented!(),
        };

        let options = ListingOptions::new(file_format)
            .with_collect_stat(state.config().collect_statistics())
            .with_target_partitions(state.config().target_partitions())
            .with_file_extension(ext);

        let table_path = ListingTableUrl::parse(location)?;
        let config = ListingTableConfig::new(table_path).with_listing_options(options);
        let table = ListingTable::try_new(config)?;

        let wrapper = ListingTableWrapper {
            inner: table,
            url: store.generate_url(),
            store: store.build_object_store()?,
        };

        Ok(Arc::new(wrapper))
    }
}

fn get_extension(path: &str) -> String {
    let res = Path::new(path).extension().and_then(|ext| ext.to_str());
    match res {
        Some(ext) => format!(".{ext}"),
        None => String::new(),
    }
}

fn file_type_from_extension(path: &str) -> Result<FileType> {
    match Path::new(path).extension().and_then(|ext| ext.to_str()) {
        Some(ext) => ext.parse(),
        None => Err(ObjectStoreSourceError::NoFileExtension),
    }
}

/// Wraps a listing table, inserting object stores as needed.
struct ListingTableWrapper {
    inner: ListingTable,
    url: Url,
    store: Arc<dyn ObjectStore>,
}

#[async_trait]
impl TableProvider for ListingTableWrapper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> DatafusionResult<TableProviderFilterPushDown> {
        #[allow(deprecated)]
        self.inner.supports_filter_pushdown(filter)
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        state
            .runtime_env()
            .object_store_registry
            .register_store(&self.url, self.store.clone());

        let plan = self.inner.scan(state, projection, filters, limit).await?;
        Ok(plan)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_extension() {
        let tests = [
            ("s3://my_bucket/file.csv", ".csv"),
            ("s3://my_bucket/file", ""),
        ];

        for test in tests {
            let ext = get_extension(test.0);
            assert_eq!(test.1, ext.as_str());
        }
    }
}
