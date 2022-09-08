//! Executors for mutations (insertions) into the database.
use crate::errors::Result;
use futures::Future;
use std::fmt::Debug;
use std::pin::Pin;

mod insert;
pub use insert::*;

pub type PinnedMutFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub trait MutExecutor: Debug + Sync + Send {
    /// Execute a mutating operation.
    fn execute(self) -> Result<PinnedMutFuture>;
}
