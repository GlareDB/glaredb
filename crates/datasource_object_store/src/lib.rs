use std::sync::Arc;

use object_store::{ObjectMeta, ObjectStore};

pub mod errors;
pub mod gcs;
pub mod local;
pub mod s3;

mod parquet;

pub trait TableAccessor: Send + Sync {
    fn store(&self) -> &Arc<dyn ObjectStore>;

    fn object_meta(&self) -> &Arc<ObjectMeta>;
}
