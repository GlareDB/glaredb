use crate::errors::{LlamaError, Result};
use async_trait::async_trait;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

#[async_trait]
pub trait FileSystem {
    // Note that this type definition requires that we synchronize reads/writes.
    // Eventually it will make sense to use pwrite/pread (on unix) to allow for
    // concurrent access.
    type File: AsyncRead + AsyncWrite + AsyncSeek;

    /// Opens a file at the given path. Creates the file if it doesn't exist.
    async fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::File>;
}
