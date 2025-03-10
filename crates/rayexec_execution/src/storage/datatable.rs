use std::fmt::Debug;

use rayexec_error::Result;

use super::projections::Projections;
use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ColumnCollectionScanState,
    ConcurrentColumnCollection,
    ParallelColumnCollectionScanState,
};
use crate::arrays::datatype::DataType;

#[derive(Debug)]
pub struct DataTableScanState {
    state: ColumnCollectionScanState,
}

#[derive(Debug)]
pub struct ParallelDataTableScanState {
    state: ParallelColumnCollectionScanState,
}

#[derive(Debug)]
pub struct DataTableAppendState {
    state: ColumnCollectionAppendState,
}

/// A currently memory-only storage backend for table data.
#[derive(Debug)]
pub struct DataTable {
    collection: ConcurrentColumnCollection,
}

impl DataTable {
    pub fn new(
        datatypes: impl IntoIterator<Item = DataType>,
        segment_size: usize,
        chunk_capacity: usize,
    ) -> Self {
        DataTable {
            collection: ConcurrentColumnCollection::new(datatypes, segment_size, chunk_capacity),
        }
    }

    pub fn init_append_state(&self) -> DataTableAppendState {
        DataTableAppendState {
            state: self.collection.init_append_state(),
        }
    }

    pub fn init_scan_state(&self) -> DataTableScanState {
        DataTableScanState {
            state: self.collection.init_scan_state(),
        }
    }

    pub fn init_parallel_scan_states(
        &self,
        num_parallel: usize,
    ) -> impl Iterator<Item = ParallelDataTableScanState> + '_ {
        self.collection
            .init_parallel_scan_states(num_parallel)
            .map(|state| ParallelDataTableScanState { state })
    }

    pub fn append_batch(&self, state: &mut DataTableAppendState, batch: &Batch) -> Result<()> {
        self.collection.append_batch(&mut state.state, batch)
    }

    pub fn flush(&self, state: &mut DataTableAppendState) -> Result<()> {
        self.collection.flush(&mut state.state)
    }

    pub fn scan(
        &self,
        projections: &Projections,
        state: &mut DataTableScanState,
        output: &mut Batch,
    ) -> Result<usize> {
        self.collection.scan(projections, &mut state.state, output)
    }

    pub fn parallel_scan(
        &self,
        projections: &Projections,
        state: &mut ParallelDataTableScanState,
        output: &mut Batch,
    ) -> Result<usize> {
        self.collection
            .parallel_scan(projections, &mut state.state, output)
    }
}
