use datafusion::error::Result;
use lance::{dataset::builder::DatasetBuilder, Dataset};
use protogen::metastore::types::options::StorageOptions;

pub async fn scan_lance_table(location: &str, options: StorageOptions) -> Result<Dataset> {
    Ok(DatasetBuilder::from_uri(location)
        .with_storage_options(options.inner.into_iter().collect())
        .load()
        .await?)
}
