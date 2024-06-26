use parquet::file::{
    footer::{decode_footer, decode_metadata},
    metadata::ParquetMetaData,
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::AsyncReadAt;

#[derive(Debug, Clone)]
pub struct Metadata {
    pub parquet_metadata: ParquetMetaData,
}

impl Metadata {
    /// Loads parquet metadata from an async source.
    pub async fn load_from(reader: &mut impl AsyncReadAt, size: usize) -> Result<Self> {
        if size < 8 {
            return Err(RayexecError::new("File size is too small"));
        }

        let mut footer = [0; 8];
        let footer_start = size - 8;
        reader.read_at(footer_start, &mut footer).await?;

        let len = decode_footer(&footer).context("failed to decode footer")?;
        if size < len + 8 {
            return Err(RayexecError::new(format!(
                "File size of {size} is less than footer + metadata {}",
                len + 8
            )));
        }

        let metadata_start = size - len - 8;
        let mut metadata = vec![0; len];
        reader.read_at(metadata_start, &mut metadata).await?;

        let metadata = decode_metadata(&metadata).context("failed to decode metadata")?;

        Ok(Metadata {
            parquet_metadata: metadata,
        })
    }
}
