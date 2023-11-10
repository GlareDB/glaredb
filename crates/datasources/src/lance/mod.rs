use datafusion::error::Result;
use lance::Dataset;
use protogen::metastore::types::options::StorageOptions;


pub async fn scan_lance_table(location: &str, _: StorageOptions) -> Result<Dataset> {
    Dataset::open(location).await.map_err(|e| e.into())
}
