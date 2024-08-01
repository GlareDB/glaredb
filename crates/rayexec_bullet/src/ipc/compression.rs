use rayexec_error::{RayexecError, Result};

use super::gen::message::CompressionType as IpcCompressionType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Zstd,
    Lz4Frame,
}

impl TryFrom<IpcCompressionType> for CompressionType {
    type Error = RayexecError;

    fn try_from(value: IpcCompressionType) -> Result<Self> {
        match value {
            IpcCompressionType::LZ4_FRAME => Ok(CompressionType::Lz4Frame),
            IpcCompressionType::ZSTD => Ok(CompressionType::Zstd),
            other => Err(RayexecError::new(format!(
                "Invalid compression type: {other:?}"
            ))),
        }
    }
}
