use std::sync::atomic::{self, AtomicU64};

use rayexec_error::Result;

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalU64};
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::collection::row::{RowAddress, RowCollection};
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::execution::operators::join::hash_table_entry::HashTableEntry;
use crate::execution::operators::join::produce_all_build_side_rows;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct JoinHashTable {
    /// Join type this hash table is for.
    join_type: JoinType,
    /// Collected data for the hash table.
    data: RowCollection,
    /// Column indices for keys on the build side.
    build_key_columns: Vec<usize>,
    /// Column indices for data we're not joining on.
    build_data_columns: Vec<usize>,
    /// Byte offset into a row for where the hash/next entry value is stored.
    build_hash_offset: usize,
}

impl JoinHashTable {
    /// Returns the row count for this hash table.
    pub fn row_count(&self) -> usize {
        unimplemented!()
    }

    /// Collects data for the build side of a join.
    ///
    /// This will hash the key columns and insert batches into the row
    /// collection.
    pub fn collect_build(&mut self, state: &mut BuildState, input: &Batch) -> Result<()> {
        // Array references: [keys, hashes/next_entry, data, matches]
        let mut arrays = Vec::with_capacity(self.data.layout().types.len());
        // Get key arrays.
        for &col_idx in &self.build_key_columns {
            arrays.push(&input.arrays[col_idx]);
        }

        // Produce hashes from key arrays. Note we only have the keys in
        // `arrays` here.
        //
        // Hashes will get replaced by a next entry in the chain when inserting
        // the hashes.
        let mut hashes = Array::try_new(&NopBufferManager, DataType::UInt64, input.num_rows())?;
        let hash_vals = PhysicalU64::get_addressable_mut(&mut hashes.data)?;
        hash_many_arrays(arrays.iter().copied(), 0..input.num_rows(), hash_vals.slice)?;

        // Append plain data columns.
        for &col_idx in &self.build_data_columns {
            arrays.push(&input.arrays[col_idx]);
        }

        // Ensure we include the "matches" initial values.
        if produce_all_build_side_rows(self.join_type) {
            // Resize to match the input rows.
            state
                .match_init
                .select(&NopBufferManager, Selection::constant(input.num_rows(), 0))?;
            arrays.push(&state.match_init);
        }

        // Now append to row collection.
        self.data.append_arrays(&arrays, input.num_rows())?;

        Ok(())
    }

    pub fn probe(&self, state: &mut HashTableScanState, rhs_keys: &Batch) -> Result<()> {
        // Hash keys.
        state.hashes.resize(rhs_keys.num_rows(), 0);
        hash_many_arrays(&rhs_keys.arrays, rhs_keys.selection(), &mut state.hashes)?;

        unimplemented!()
    }

    /// Attempts to insert a new row entry into an existing entry that we expect
    /// to be empty.
    ///
    /// Returns a bool indicating if the write succeeded. If `false`, then a
    /// separate thread wrote to the same entry that we were writing to.
    fn insert_empty(&self, curr_ent: &AtomicU64, row: RowAddress, row_hash: u64) -> bool {
        let dir_ent = HashTableEntry::new(row, row_hash);

        // Attempt to swap out the entry that we expect to be empty.
        let did_write = curr_ent
            .compare_exchange(
                HashTableEntry::DANGLING_U64,
                dir_ent.as_u64(),
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_ok();

        did_write
    }

    /// Attempts to insert a new row entry at the beginning of the chain.
    fn insert_occupied(&self, curr_ent: &AtomicU64, row: RowAddress, row_hash: u64) {
        let dir_ent = HashTableEntry::new(row, row_hash);

        // SAFETY: ...
        //
        // An assumption is made that we're only ever generating valid row
        // addresses when inserting into the hash table.
        let row_ptr = unsafe { self.data.row_ptr(row) };

        let ent = curr_ent.load(atomic::Ordering::Relaxed);
        loop {}

        unimplemented!()
    }

    /// Gets a raw pointer for for getting the next entry for a row when
    /// following the hash chain.
    ///
    /// # Safety
    ///
    /// The row address must point to a valid row in the collected row data.
    unsafe fn next_entry_ptr_mut(&self, row: RowAddress) -> *const u64 {
        let row_ptr = self.data.row_ptr(row);
        let next_ent_ptr = row_ptr.byte_add(self.build_hash_offset);
        next_ent_ptr.cast()
    }
}

/// State provided during hash table build.
#[derive(Debug)]
pub struct BuildState {
    /// Initial values to use for left/outer joins for a "match".
    ///
    /// When we insert into the hash table, we'll use these values (all false)
    /// to initialize matches.
    match_init: Array,
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
