use futures::future::BoxFuture;
use rayexec_error::Result;
use std::{fmt::Debug, path::Path};

use crate::AsyncReader;

/// Provides access to a filesystem (real or virtual).
pub trait FileSystemProvider: Debug + Sync + Send + 'static {
    fn reader(&self, path: &Path) -> Result<Box<dyn FileReader>>;
}

// TODO: Unify this with HttpReader at some point. The operations we want to be
// able to do are the same.
pub trait FileReader: AsyncReader {
    fn size(&mut self) -> BoxFuture<Result<usize>>;
}
