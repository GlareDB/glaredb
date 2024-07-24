use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::task::Context;

use crate::execution::operators::{PollFinalize, PollPush};

pub trait DataTable: Debug + Sync + Send {
    /// Return table scanners for the table.
    ///
    /// The provided `num_partitions` argument is the desired number of
    /// partitions in the table output. However, the table may return a
    /// different number of partitions if it's unable to use the provided
    /// number.
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>>;

    fn insert(&self, _input_partitions: usize) -> Result<Vec<Box<dyn DataTableInsert>>> {
        Err(RayexecError::new("Data table does not support inserts"))
    }

    fn update(&self, _input_partitions: usize) -> Result<Vec<Box<dyn DataTableUpdate>>> {
        Err(RayexecError::new("Data table does not support updates"))
    }

    fn delete(&self, _input_partitions: usize) -> Result<Vec<Box<dyn DataTableDelete>>> {
        Err(RayexecError::new("Data table does not support updates"))
    }
}

pub trait DataTableScan: Debug + Send {
    /// Pull the next batch in the scan.
    ///
    /// Returns None if the scan is exhausted.
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>>;
}

/// Implementation of `DataTableScan` that immediately returns exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyTableScan;

impl DataTableScan for EmptyTableScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async move { Ok(None) })
    }
}

pub trait DataTableInsert: Debug + Sync + Send {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush>;
    fn poll_finalize_push(&mut self, cx: &mut Context) -> Result<PollFinalize>;
}

pub trait DataTableUpdate: Debug + Sync + Send {}

pub trait DataTableDelete: Debug + Sync + Send {}
