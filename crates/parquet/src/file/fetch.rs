use rayexec_error::Result;
use rayexec_io::FileSource;

use super::metadata::ParquetMetaData;

/// Asynchronously fetch the parquet metadata from the file source.
pub async fn fetch_metadata(source: &dyn FileSource) -> Result<ParquetMetaData> {
    unimplemented!()
    // let mut footer = [0; 8];
    // source.

    // unimplemented!()
}
