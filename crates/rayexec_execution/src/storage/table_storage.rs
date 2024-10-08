use std::fmt::Debug;

use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};

use crate::database::catalog_entry::CatalogEntry;
use crate::execution::operators::sink::PartitionSink;

/// Scan projections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projections {
    /// Column indices to project out of the scan.
    ///
    /// If None, project all columns.
    pub column_indices: Option<Vec<usize>>,
}

impl Projections {
    pub const fn all() -> Self {
        Projections {
            column_indices: None,
        }
    }
}

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
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>>;

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

/// Helper for wrapping an unprojected scan with a projections list to produce
/// projected batches.
///
/// This is inefficient compared to handling the projection in the scan itself
/// since this projects a batch after it's already been read.
#[derive(Debug)]
pub struct ProjectedScan<S> {
    pub projections: Projections,
    pub scan: S,
}

impl<S: DataTableScan> ProjectedScan<S> {
    pub fn new(scan: S, projections: Projections) -> Self {
        ProjectedScan { projections, scan }
    }

    async fn pull_inner(&mut self) -> Result<Option<Batch>> {
        let batch = match self.scan.pull().await? {
            Some(batch) => batch,
            None => return Ok(None),
        };

        match self.projections.column_indices.as_ref() {
            Some(indices) => {
                let batch = batch.project(indices);
                Ok(Some(batch))
            }
            None => Ok(Some(batch)),
        }
    }
}

impl<S: DataTableScan> DataTableScan for ProjectedScan<S> {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async { self.pull_inner().await })
    }
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
