use bytes::Bytes;
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::ParquetMetaData;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::FileSource;
use tracing::trace;

#[derive(Debug, Clone)]
pub struct Metadata {
    pub decoded_metadata: ParquetMetaData,
    /// The metadata in its original serialized form.
    ///
    /// Storing this allows us to repeatedly deserialize the metadata without
    /// needing to read from the file again. This is needed during hybrid/dist
    /// exec.
    pub metadata_buffer: Bytes,
}

impl Metadata {
    /// Loads parquet metadata from an async source.
    pub async fn new_from_source(reader: &mut dyn FileSource, size: usize) -> Result<Self> {
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
        let metadata_buffer = reader.read_range(metadata_start, len).await?;

        let metadata = decode_metadata(&metadata_buffer).context("failed to decode metadata")?;

        Ok(Metadata {
            decoded_metadata: metadata,
            metadata_buffer,
        })
    }

    pub fn try_from_buffer(buf: Bytes) -> Result<Self> {
        let metadata = decode_metadata(&buf).context("failed to decode metadata")?;

        Ok(Metadata {
            decoded_metadata: metadata,
            metadata_buffer: buf,
        })
    }
}
