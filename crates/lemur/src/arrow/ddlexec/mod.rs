//! Executors for (physical) DDL operations against the database.
use crate::errors::Result;
use futures::Future;
use std::fmt::Debug;
use std::pin::Pin;

mod table;

pub use table::*;

pub type PinnedDdlFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub trait DdlExecutor: Debug + Sync + Send {
    /// Execute a (physical) ddl operation.
    fn execute(self) -> Result<PinnedDdlFuture>;
}
