use crate::errors::ProtoConvError;
use crate::gen::rpcsrv::common;

#[derive(Debug, Clone)]
pub struct SessionStorageConfig {
    pub gcs_bucket: Option<String>,
}

impl TryFrom<common::SessionStorageConfig> for SessionStorageConfig {
    type Error = ProtoConvError;
    fn try_from(value: common::SessionStorageConfig) -> Result<Self, Self::Error> {
        Ok(SessionStorageConfig {
            gcs_bucket: value.gcs_bucket,
        })
    }
}

impl From<SessionStorageConfig> for common::SessionStorageConfig {
    fn from(value: SessionStorageConfig) -> Self {
        common::SessionStorageConfig {
            gcs_bucket: value.gcs_bucket,
        }
    }
}
