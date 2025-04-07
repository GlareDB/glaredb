use glaredb_error::Result;

use super::ParquetMetaData;

#[derive(Debug)]
pub struct MetaDataLoader {
    // TODO: Caching and stuff.
}

impl MetaDataLoader {
    pub async fn load_from_file<F>(_file: &mut F) -> Result<ParquetMetaData> {
        unimplemented!()
    }
}
