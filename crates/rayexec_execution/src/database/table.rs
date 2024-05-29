use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::task::Context;

use crate::execution::operators::{PollPull, PollPush};

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

pub trait DataTableScan: Debug + Sync + Send {
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull>;
}

pub trait DataTableInsert: Debug + Sync + Send {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush>;
    fn finalize(&mut self) -> Result<()>;
}

pub trait DataTableUpdate: Debug + Sync + Send {}

pub trait DataTableDelete: Debug + Sync + Send {}
