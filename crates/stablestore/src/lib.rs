//! Stable storage.
pub mod errors;
pub mod object_store;

use async_trait::async_trait;
use errors::Result;
use serde::{Deserialize, Serialize};

/// Each stable storage blob has an associated version.
pub type Version = u64;

pub trait Blob: Serialize + for<'de> Deserialize<'de> + Send + Sync {}

/// Option for specifying which version of a file to read.
#[derive(Debug, Clone)]
pub enum VersionReadOption {
    /// Read the latest version.
    Latest,
    /// Read a specific version.
    Version(Version),
}

/// The main stable storage trait.
///
/// Stable storage should be used for various pieces of metadata for the
/// database (including the catalog).
#[async_trait]
pub trait StableStorage: Sync + Send {
    /// Read some version of a named blob.
    ///
    /// Returns `Ok(None)` if an object doesn't exist with that version.
    async fn read<B: Blob>(&self, name: &str, opt: VersionReadOption) -> Result<Option<B>>;

    /// Append a blob.
    ///
    /// The version that the blob was written with will be returned.
    ///
    /// Previous versions of the blob will not be overwritten. Blobs are
    /// guaranteed to be durable once this completes.
    async fn append<B: Blob>(&self, name: &str, blob: &B) -> Result<Version>;
}
