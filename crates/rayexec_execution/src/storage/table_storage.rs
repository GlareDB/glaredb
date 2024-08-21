use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;

use crate::{database::catalog_entry::CatalogEntry, execution::operators::sink::PartitionSink};

pub trait TableStorage: Debug + Sync + Send {
    fn data_table(&self, schema: &str, ent: &CatalogEntry) -> Result<Box<dyn DataTable>>;

    fn create_physical_table(
        &self,
        schema: &str,
        ent: &CatalogEntry,
    ) -> BoxFuture<'_, Result<Box<dyn DataTable>>>;

    fn drop_physical_table(&self, schema: &str, ent: &CatalogEntry) -> BoxFuture<'_, Result<()>>;
}

pub trait DataTable: Debug + Sync + Send {
    /// Return table scanners for the table.
    ///
    /// The provided `num_partitions` argument is the desired number of
    /// partitions in the table output. However, the table may return a
    /// different number of partitions if it's unable to use the provided
    /// number.
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>>;

    fn insert(&self, _input_partitions: usize) -> Result<Vec<Box<dyn PartitionSink>>> {
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

pub trait DataTableUpdate: Debug + Sync + Send {}

pub trait DataTableDelete: Debug + Sync + Send {}
