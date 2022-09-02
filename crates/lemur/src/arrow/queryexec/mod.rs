use crate::arrow::chunk::Chunk;
use crate::errors::{LemurError, Result};
use futures::Stream;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

/// Types that stream dataframes.
pub trait DataFrameStream: Stream<Item = Result<Chunk>> {}

pub type PinnedDataFrameStream = Pin<Box<dyn DataFrameStream + Send>>;

pub trait QueryExecutor: Debug + Sync + Send {
    /// Return a possibly empty list of children for this executor.
    fn children(&self) -> &[Arc<dyn QueryExecutor>];

    /// Begin execution.
    fn execute(&self) -> Result<PinnedDataFrameStream>;
}
