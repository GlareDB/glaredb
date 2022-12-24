//! Stable storage.
pub mod errors;
pub mod object_store;

use async_trait::async_trait;
use errors::Result;
use serde::{Deserialize, Serialize};

/// Each stable storage blob has an associated version.
pub type Version = u64;

pub trait Blob: Serialize + for<'de> Deserialize<'de> + Send + Sync {}

/// The main stable storage trait.
///
/// Stable storage should be used for various pieces of metadata for the
/// database (including the catalog).
#[async_trait]
pub trait StableStorage: Send {
    /// Read the latest blob for some name.
    async fn latest<B: Blob>(&self, name: &str) -> Result<B>;

    /// Append a blob.
    ///
    /// The version that the blob was written with will be returned.
    ///
    /// Previous versions of the blob will not be overwritten. Blobs are
    /// guaranteed to be durable once this completes.
    async fn append<B: Blob>(&self, name: &str, blob: &B) -> Result<Version>;
}
