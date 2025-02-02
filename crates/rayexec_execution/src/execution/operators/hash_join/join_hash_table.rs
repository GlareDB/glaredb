use std::sync::atomic::{self, AtomicBool, AtomicPtr, AtomicU64};

use rayexec_error::Result;

use super::hash_table_scan::HashTableScanState;
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalU64, ScalarStorage};
use crate::arrays::array::raw::TypedRawBuffer;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::collection::row::{RowAppendState, RowCollection};
use crate::arrays::collection::row_layout::RowLayout;
use crate::arrays::collection::row_matcher::PredicateRowMatcher;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct JoinHashTable {
    /// Join type this hash table is for.
    pub join_type: JoinType,
    /// Collected data for the hash table.
    pub data: RowCollection,
    /// Hash table entries pointing to rows in the data collection.
    ///
    /// Initialize after we collect all data.
    pub directory: Option<Directory>,
    /// Column indices for keys on the build side.
    pub build_key_columns: Vec<usize>,
    /// Column indices for data we're not joining on.
    pub build_data_columns: Vec<usize>,
    /// Byte offset into a row for where the hash/next entry value is stored.
    pub build_hash_byte_offset: usize,
    /// Configured batch size for the join operator.
    pub batch_size: usize,
    /// Matcher for evaluating predicates on the rows.
    pub row_matcher: PredicateRowMatcher,
}

impl JoinHashTable {
    pub fn new(
        join_type: JoinType,
        datatypes: impl IntoIterator<Item = DataType>,
        build_key_columns: impl IntoIterator<Item = usize>,
        build_data_columns: impl IntoIterator<Item = usize>,
        batch_size: usize,
    ) -> Self {
        let build_key_columns: Vec<_> = build_key_columns.into_iter().collect();
        let build_data_columns: Vec<_> = build_data_columns.into_iter().collect();
        let mut datatypes: Vec<_> = datatypes.into_iter().collect();

        // Insert hash datatype in the right spot, immediately after keys.
        datatypes.insert(build_key_columns.len(), DataType::UInt64);

        // Append extra boolean column if were a LEFT/OUTER join to track
        // matches.
        if join_type.produce_all_build_side_rows() {
            datatypes.push(DataType::Boolean);
        }

        let layout = RowLayout::new(datatypes);
        let build_hash_byte_offset = layout.offsets[build_key_columns.len()];
        let data = RowCollection::new(layout, batch_size);

        JoinHashTable {
            join_type,
            data,
            directory: None,
            batch_size,
            build_key_columns,
            build_data_columns,
            build_hash_byte_offset,
            row_matcher: PredicateRowMatcher {}, // TODO
        }
    }

    /// Initializes a build state for this hash table.
    pub fn init_build_state(&self) -> BuildState {
        BuildState {
            match_init: Array::try_new_constant(&NopBufferManager, &false.into(), self.batch_size)
                .expect("constant array to build"),
            row_append: self.data.init_append(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.data.row_count()
    }

    /// Collects data for the build side of a join.
    ///
    /// This will hash the key columns and insert batches into the row
    /// collection.
    pub fn collect_build(&mut self, state: &mut BuildState, input: &Batch) -> Result<()> {
        // Array references: [keys, hashes/next_entry, data, matches]
        //
        // The hashes/next_entry column is initially all hash values. Once we've
        // collected all build-side data, we overwrite the hash values with
        // pointers to the next entry in the chain.
        //
        // Note that the hashes/next_entry column is stored as 64 bits. For
        // systems that use 32 bit pointers (wasm), this should still function
        // correctly. The first 32 bits will be the actual pointer, the trailing
        // 32 bits will be meaningless data (that we don't read).
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
        arrays.push(&hashes);

        // Append plain data columns.
        for &col_idx in &self.build_data_columns {
            arrays.push(&input.arrays[col_idx]);
        }

        // Ensure we include the "matches" initial values.
        if self.join_type.produce_all_build_side_rows() {
            // Resize to match the input rows.
            state
                .match_init
                .select(&NopBufferManager, Selection::constant(input.num_rows(), 0))?;
            arrays.push(&state.match_init);
        }

        // Now append to row collection.
        self.data
            .append_arrays(&mut state.row_append, &arrays, input.num_rows())?;

        Ok(())
    }

    /// Initialize the directory for the hash table.
    ///
    /// This should only be done once and after all build-side data has been
    /// collected.
    pub fn init_directory(&mut self) -> Result<()> {
        let num_rows = self.data.row_count();
        let directory = Directory::new_for_num_rows(num_rows)?;
        self.directory = Some(directory);

        Ok(())
    }

    /// Inserts hashes for the given blocks into the hash table.
    ///
    /// This should be called after all data has been collected for the build
    /// side, and the directory having been initialized.
    ///
    /// Each thread will have a set of blocks that it's responsible for
    /// inserting. All blocks need to be handled prior to probing the hash
    /// table.
    ///
    /// This can be called concurrently by multiple threads. Entries in the hash
    /// table are atomically updated.
    pub fn insert_hashes_for_blocks(
        &self,
        block_indices: impl IntoIterator<Item = usize>,
    ) -> Result<()> {
        let mut hashes = Array::try_new(&NopBufferManager, DataType::UInt64, self.batch_size)?;
        let mut scan_state = self.data.init_partial_scan(block_indices);

        let scan_cols = &[self.hash_column()];

        loop {
            let count = self.data.scan_columns(
                &mut scan_state,
                scan_cols,
                &mut [&mut hashes],
                self.batch_size,
            )?;

            if count == 0 {
                // No more hashes to scan.
                break;
            }

            // Hashes should always be valid.
            debug_assert!(hashes.validity.all_valid());

            let hashes = PhysicalU64::get_addressable(&hashes.data)?;
            let hashes = &hashes.slice[0..count];

            self.insert_hashes(hashes, scan_state.scanned_row_pointers())?;
        }

        Ok(())
    }

    /// Inserts hashes into the hash table.
    fn insert_hashes(&self, hashes: &[u64], row_pointers: &[*const u8]) -> Result<()> {
        debug_assert_eq!(hashes.len(), row_pointers.len());

        let directory = &self
            .directory
            .as_ref()
            .expect("directory to be initialized");

        // Compute positions for each entry using the hashes.
        let pos_mask = directory.capacity_mask();
        let positions = hashes
            .iter()
            .copied()
            .map(|hash| (hash & pos_mask) as usize);

        for (pos, &row_ptr) in positions.zip(row_pointers) {
            let atomic_ptr = directory.get_entry_atomic(pos);
            let current_ent = atomic_ptr.load(atomic::Ordering::Relaxed);

            if current_ent.is_null() {
                // Entry is free, try to insert into it.
                if self.insert_empty(atomic_ptr, row_ptr) {
                    // We inserted, move to next row to insert.
                    continue;
                }
            }

            // Either the entry isn't dangling, or we failed to insert into
            // empty entry ( it became occupied as we tried to insert).
            self.insert_occupied(atomic_ptr, row_ptr);
        }

        Ok(())
    }

    /// Probe the hash table with the given keys.
    ///
    /// The scan state will be updated for scanning.
    pub fn probe(
        &self,
        state: &mut HashTableScanState,
        rhs_keys: &[Array],
        count: usize,
    ) -> Result<()> {
        // Hash keys.
        state.hashes.resize(count, 0);
        hash_many_arrays(rhs_keys, 0..count, &mut state.hashes)?;

        // Resize entries to number of keys we're probing with. These will be
        // overwritten in the below loop.
        state.row_pointers.resize(count, std::ptr::null());

        let directory = &self
            .directory
            .as_ref()
            .expect("directory to be initialized");

        // Compute positions for each entry using the hashes.
        let pos_mask = directory.capacity_mask();
        let positions = state
            .hashes
            .iter()
            .copied()
            .map(|hash| (hash & pos_mask) as usize);

        // Store entries on the scan state. We'll do the equality/comparison
        // checking during the actual scan call.
        for (row_ptr, position) in state.row_pointers.iter_mut().zip(positions) {
            *row_ptr = directory.get_entry(position);
        }

        Ok(())
    }

    /// Attempts to insert a new row entry into an existing entry that we expect
    /// to be empty.
    ///
    /// Returns a bool indicating if the write succeeded. If `false`, then a
    /// separate thread wrote to the same entry that we were writing to.
    ///
    /// # Safety
    ///
    /// This requires that we have no outstanding byte slice references, and
    /// that every pointer we write to in the row collection is non-overlapping.
    /// The logic of the hash table should ensure that each thread is writing to
    /// separate set of rows.
    fn insert_empty(&self, atomic_ent: &AtomicPtr<u8>, new_ent: *const u8) -> bool {
        // Update next entry for this chain to null.
        // SAFETY: ...
        unsafe {
            self.write_next_entry_ptr(new_ent, std::ptr::null());
        }

        // Attempt to swap out the entry that we expect to be empty.
        let did_write = atomic_ent
            .compare_exchange(
                std::ptr::null_mut(),
                new_ent.cast_mut(),
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_ok();

        did_write
    }

    /// Attempts to insert a new row entry at the beginning of the chain.
    ///
    /// # Safety
    ///
    /// See `insert_empty`.
    fn insert_occupied(&self, atomic_ent: &AtomicPtr<u8>, new_ent: *const u8) {
        let mut curr_ent = atomic_ent.load(atomic::Ordering::Relaxed);
        loop {
            // Update pointer to next entry in chain.
            // SAFETY: ...
            //
            // An assumption is made that we're only ever generating valid row
            // addresses when inserting into the hash table.
            unsafe {
                self.write_next_entry_ptr(new_ent, curr_ent);
            };

            // Now try to update the atomic entry to point to this row.
            match atomic_ent.compare_exchange_weak(
                curr_ent,
                new_ent.cast_mut(),
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break,                       // Success.
                Err(existing) => curr_ent = existing, // Try again.
            }
        }
    }

    /// Writes to the rows indicating that they were matched in a probe.
    ///
    /// This is used to mark rows as matched during LEFT/OUTER joins.
    ///
    /// # Safety
    ///
    /// The pointers must point to the beginning of rows in the collection.
    ///
    /// The 'matches' column data must not be concurrently accessed outside of
    /// this function. This should hold as probing never touches this column,
    /// even when executing predicates.
    pub unsafe fn write_rows_matched(&self, row_ptrs: impl IntoIterator<Item = *const u8>) {
        let match_offset = *self
            .data
            .layout()
            .offsets
            .last()
            .expect("match offset to exist");

        for row_ptr in row_ptrs {
            // Note the morsels paper says it's advantageous to check the bool
            // before setting it to avoid contention. I'm assuming they mean
            // without atomic access. That's technically UB, and miri would
            // complain. So just do it atomically.
            let match_ptr = row_ptr.byte_add(match_offset).cast_mut().cast::<bool>();
            let match_bool = AtomicBool::from_ptr(match_ptr);
            match_bool.store(true, atomic::Ordering::Relaxed);
        }
    }

    unsafe fn write_next_entry_ptr(&self, row_ptr: *const u8, next_ent: *const u8) {
        let next_ent_ptr = row_ptr
            .byte_add(self.build_hash_byte_offset)
            .cast::<*const u8>()
            .cast_mut();
        next_ent_ptr.write_unaligned(next_ent);
    }

    pub unsafe fn read_next_entry_ptr(&self, row_ptr: *const u8) -> *const u8 {
        let next_ent_ptr = row_ptr
            .byte_add(self.build_hash_byte_offset)
            .cast::<*const u8>();
        next_ent_ptr.read_unaligned()
    }

    /// Index to the hash column the row collection.
    ///
    /// The hash/next entry columns is always the first column after the keys.
    fn hash_column(&self) -> usize {
        self.build_key_columns.len()
    }
}

/// (Chained) hash table directory.
///
/// Each entry is a row pointer pointing to the front of a chain. Each row will
/// point to the next entry in the chain through a serialized pointer. The end
/// of a chain is denoted by a null pointer.
#[derive(Debug)]
pub struct Directory {
    entries: TypedRawBuffer<*mut u8, NopBufferManager>,
}

impl Directory {
    const MIN_SIZE: usize = 256;
    const LOAD_FACTOR: f64 = 0.7;

    fn empty() -> Self {
        Directory {
            entries: TypedRawBuffer::try_with_capacity(&NopBufferManager, 0).unwrap(),
        }
    }

    /// Mask to use when determining the position for an entry in the hash
    /// table.
    const fn capacity_mask(&self) -> u64 {
        self.entries.capacity() as u64 - 1
    }

    /// Create a new directory for the given number of rows.
    ///
    /// This will ensure the hash table is an appropriate size and that the size
    /// is a power of two for efficient computing of offsets.
    fn new_for_num_rows(num_rows: usize) -> Result<Self> {
        let desired = (num_rows as f64 / Self::LOAD_FACTOR) as usize;
        let actual = usize::max(desired.next_power_of_two(), Self::MIN_SIZE);

        let mut entries = TypedRawBuffer::try_with_capacity(&NopBufferManager, actual)?;
        entries.as_slice_mut().fill(std::ptr::null_mut());

        Ok(Directory { entries })
    }

    fn get_entry(&self, idx: usize) -> *const u8 {
        debug_assert!(idx < self.entries.capacity());
        let ptr = unsafe { self.entries.as_ptr().add(idx) };
        unsafe { *ptr }
    }

    fn get_entry_atomic(&self, idx: usize) -> &AtomicPtr<u8> {
        debug_assert!(idx < self.entries.capacity());
        let ptr = unsafe { self.entries.as_mut_ptr().add(idx) };
        unsafe { AtomicPtr::from_ptr(ptr) }
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
    /// State for appending rows to the collection.
    row_append: RowAppendState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::testutil::generate_batch;

    #[test]
    fn insert_key_only() {
        let mut table = JoinHashTable::new(JoinType::Inner, [DataType::Int32], [0], [], 16);
        let mut build_state = table.init_build_state();

        let input = generate_batch!([1, 2, 3, 4]);
        table.collect_build(&mut build_state, &input).unwrap();

        table.init_directory().unwrap();
        table.insert_hashes_for_blocks([0]).unwrap();
    }
}
