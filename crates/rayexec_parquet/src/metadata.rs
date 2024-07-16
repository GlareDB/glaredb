use parquet::file::{
    footer::{decode_footer, decode_metadata},
    metadata::ParquetMetaData,
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::FileSource;
use tracing::trace;

#[derive(Debug, Clone)]
pub struct Metadata {
    pub parquet_metadata: ParquetMetaData,
}

impl Metadata {
    /// Loads parquet metadata from an async source.
    pub async fn load_from(reader: &mut dyn FileSource, size: usize) -> Result<Self> {
        if size < 8 {
            return Err(RayexecError::new("File size is too small"));
        }

        let footer_start = size - 8;
        let footer = reader.read_range(footer_start, 8).await?;
        trace!("read parquet footer bytes");

        let len = decode_footer(footer.as_ref().try_into().unwrap())
            .context("failed to decode footer")?;
        if size < len + 8 {
            return Err(RayexecError::new(format!(
                "File size of {size} is less than footer + metadata {}",
                len + 8
            )));
        }

        let metadata_start = size - len - 8;
        let metadata = reader.read_range(metadata_start, len).await?;

        let metadata = decode_metadata(&metadata).context("failed to decode metadata")?;

        Ok(Metadata {
            parquet_metadata: metadata,
        })
    }
}
