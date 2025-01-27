use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct JoinHashTable {}

impl JoinHashTable {
    /// Returns the row count for this hash table.
    pub fn row_count(&self) -> usize {
        unimplemented!()
    }

    pub fn probe(&self, state: &mut HashTableScanState, rhs_keys: &Batch) -> Result<()> {
        // Hash keys.
        state.hashes.resize(rhs_keys.num_rows(), 0);
        hash_many_arrays(&rhs_keys.arrays, rhs_keys.selection(), &mut state.hashes)?;

        unimplemented!()
    }
}

/// Scan state for resuming probes of the hash table.
#[derive(Debug)]
pub struct HashTableScanState {
    /// Reusable hashes buffer. Filled when we probe the hash table with
    /// rhs_keys.
    pub(crate) hashes: Vec<u64>,
}

impl HashTableScanState {
    pub fn scan(
        &mut self,
        join_type: JoinType,
        table: &JoinHashTable,
        rhs_keys: &Batch,
        output: &mut Batch,
    ) -> Result<()> {
        match join_type {
            JoinType::Inner => self.scan_inner_join(table, rhs_keys, output),
            _ => unimplemented!(),
        }
    }

    pub fn scan_inner_join(
        &mut self,
        table: &JoinHashTable,
        rhs_keys: &Batch,
        output: &mut Batch,
    ) -> Result<()> {
        unimplemented!()
    }
}
