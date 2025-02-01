use std::sync::atomic::{self, AtomicU64};

use rayexec_error::Result;

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalU64, ScalarStorage};
use crate::arrays::array::raw::TypedRawBuffer;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::collection::row::{RowAddress, RowCollection};
use crate::arrays::collection::row_layout::RowLayout;
use crate::arrays::collection::row_matcher::PredicateRowMatcher;
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
    /// Hash table entries pointing to rows in the data collection.
    ///
    /// Initialize after we collect all data.
    directory: Option<Directory>,
    /// Column indices for keys on the build side.
    build_key_columns: Vec<usize>,
    /// Column indices for data we're not joining on.
    build_data_columns: Vec<usize>,
    /// Byte offset into a row for where the hash/next entry value is stored.
    build_hash_byte_offset: usize,
    /// Configured batch size for the join operator.
    batch_size: usize,
    /// Matcher for evaluating predicates on the rows.
    row_matcher: PredicateRowMatcher,
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

    pub fn init_build_state(&self) -> BuildState {
        BuildState {
            match_init: Array::try_new_constant(&NopBufferManager, &false.into(), self.batch_size)
                .expect("constant array to build"),
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
        if produce_all_build_side_rows(self.join_type) {
            // Resize to match the input rows.
            state
                .match_init
                .select(&NopBufferManager, Selection::constant(input.num_rows(), 0))?;
            arrays.push(&state.match_init);
        }

        unimplemented!()
        // // Now append to row collection.
        // self.data.append_arrays(&arrays, input.num_rows())?;

        // Ok(())
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

    /// Inserts hashes for the given chunks into the hash table.
    ///
    /// This should be called after all data has been collected for the build
    /// side, and the directory having been initialized.
    ///
    /// Each thread will have a set of chunks that they're responsible for
    /// inserting. All chunks need to be handled prior to probing the hash
    /// table.
    ///
    /// This can be called concurrently by multiple threads. Entries in the hash
    /// table are atomically updated.
    pub fn insert_hashes_for_chunks(
        &self,
        chunk_indices: impl IntoIterator<Item = usize>,
    ) -> Result<()> {
        let mut hashes = Array::try_new(&NopBufferManager, DataType::UInt64, self.batch_size)?;
        let mut scan_state = self.data.init_partial_scan(chunk_indices);

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

            self.insert_hashes(hashes, scan_state.row_addresses())?;
        }

        Ok(())
    }

    /// Inserts hashes into the hash table.
    fn insert_hashes(&self, hashes: &[u64], row_addresses: &[RowAddress]) -> Result<()> {
        debug_assert_eq!(hashes.len(), row_addresses.len());

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

        let entries = row_addresses
            .iter()
            .copied()
            .zip(hashes.iter().copied())
            .map(|(addr, hash)| HashTableEntry::new(addr, hash));

        for (pos, new_ent) in positions.zip(entries) {
            let atomic_ent = directory.get_entry_as_atomic_u64(pos);
            let current_ent = atomic_ent.load(atomic::Ordering::Relaxed);

            if current_ent == HashTableEntry::DANGLING_U64 {
                // Entry is free, try to insert into it.
                if self.insert_empty(atomic_ent, new_ent) {
                    // We inserted, move to next row to insert.
                    continue;
                }
            }

            // Either the entry isn't dangling, or we failed to insert into
            // empty entry ( it became occupied as we tried to insert).
            self.insert_occupied(atomic_ent, new_ent);
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
        state.entries.resize(count, HashTableEntry::DANGLING);

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
        for (matching_ent, position) in state.entries.iter_mut().zip(positions) {
            *matching_ent = directory.get_entry(position);
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
    fn insert_empty(&self, atomic_ent: &AtomicU64, new_ent: HashTableEntry) -> bool {
        // SAFETY: ...
        let chain_ptr = unsafe { self.next_entry_ptr(new_ent.row_address()) }.cast_mut();

        // Write dangling to indicate end of chain.
        unsafe { chain_ptr.write_unaligned(HashTableEntry::DANGLING_U64) };

        // Attempt to swap out the entry that we expect to be empty.
        let did_write = atomic_ent
            .compare_exchange(
                HashTableEntry::DANGLING_U64,
                new_ent.as_u64(),
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
    fn insert_occupied(&self, atomic_ent: &AtomicU64, new_ent: HashTableEntry) {
        // SAFETY: ...
        //
        // An assumption is made that we're only ever generating valid row
        // addresses when inserting into the hash table.
        let chain_ptr = unsafe { self.next_entry_ptr(new_ent.row_address()) }.cast_mut();

        let mut curr_ent = atomic_ent.load(atomic::Ordering::Relaxed);
        loop {
            // Update pointer to next entry in chain.
            unsafe { chain_ptr.write_unaligned(curr_ent) };

            // Now try to update the atomic entry to point to this row.
            match atomic_ent.compare_exchange_weak(
                curr_ent,
                new_ent.as_u64(),
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break,                       // Success.
                Err(existing) => curr_ent = existing, // Try again.
            }
        }
    }

    /// Gets a raw pointer for for getting the next entry for a row when
    /// following the hash chain.
    ///
    /// The returns pointer is not guaranteed to be aligned.
    ///
    /// # Safety
    ///
    /// The row address must point to a valid row in the collected row data.
    unsafe fn next_entry_ptr(&self, row: RowAddress) -> *const u64 {
        let row_ptr = self.data.row_ptr(row);
        let next_ent_ptr = row_ptr.byte_add(self.build_hash_byte_offset);
        next_ent_ptr.cast()
    }

    /// Reads the next entry for the a given row.
    ///
    /// # Safety
    ///
    /// Same concerns as `next_entry_ptr`.
    unsafe fn read_next_entry(&self, row: RowAddress) -> HashTableEntry {
        let ptr = self.next_entry_ptr(row);
        let v = ptr.read_unaligned();
        HashTableEntry::from_u64(v)
    }

    /// Index to the hash column the row collection.
    ///
    /// The hash/next entry columns is always the first column after the keys.
    fn hash_column(&self) -> usize {
        self.build_key_columns.len()
    }
}

/// Hash table directory.
#[derive(Debug)]
struct Directory {
    entries: TypedRawBuffer<HashTableEntry, NopBufferManager>,
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
        entries
            .as_slice_mut()
            .iter_mut()
            .for_each(|ent| *ent = HashTableEntry::DANGLING);

        Ok(Directory { entries })
    }

    fn get_entry(&self, idx: usize) -> HashTableEntry {
        debug_assert!(idx < self.entries.capacity());
        let ptr = unsafe { self.entries.as_ptr().add(idx) };
        unsafe { *ptr }
    }

    fn get_entry_as_atomic_u64(&self, idx: usize) -> &AtomicU64 {
        debug_assert!(idx < self.entries.capacity());
        let ptr = unsafe { self.entries.as_ptr().add(idx) };
        let ptr = ptr.cast::<AtomicU64>();
        unsafe { &*ptr }
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
    /// Selection to use for entries that we're still scanning.
    ///
    /// Once this is empty, we know we're done scanning this set of keys.
    selection: Vec<usize>,
    /// Current set of entries we're working with.
    entries: Vec<HashTableEntry>,
    /// Reusable hashes buffer. Filled when we probe the hash table with
    /// rhs_keys.
    hashes: Vec<u64>,
    /// Selection of rows from the rhs that we've matched for a scan. Resusable
    /// buffer.
    predicates_matched: Vec<usize>,
}

impl HashTableScanState {
    pub fn scan(
        &mut self,
        table: &JoinHashTable,
        rhs_keys: &Batch,
        output: &mut Batch,
    ) -> Result<()> {
        match table.join_type {
            _ => unimplemented!(),
        }
    }

    pub fn scan_inner_join(
        &mut self,
        table: &JoinHashTable,
        rhs_keys: &[Array],
        rhs: &[Array],
        output: &mut Batch,
    ) -> Result<()> {
        if self.selection.is_empty() {
            // Done, need new of keys.
            output.set_num_rows(0)?;
            return Ok(());
        }

        self.predicates_matched.clear();
        loop {
            // Rows to read from the left.
            let lhs_rows = self
                .selection
                .iter()
                .map(|&row_idx| self.entries[row_idx].row_address());

            // Rows to read from the right.
            let rhs_rows = self.selection.iter().copied();

            table.row_matcher.find_matches(
                &table.data,
                lhs_rows,
                &table.build_key_columns,
                rhs_keys,
                rhs_rows,
                &mut self.predicates_matched,
            )?;

            if !self.predicates_matched.is_empty() {
                // Predicates matched, need to produce output.
                break;
            }

            // Otherwise none of the predicates matched, move to next entries.
            self.follow_next_in_chain(table);
            if self.selection.is_empty() {
                // We're at the end of all chains, nothing more to read.
                output.set_num_rows(0)?;
                return Ok(());
            }
        }

        unimplemented!()
    }

    /// For each entry in the current scan state, follow the chain to load the
    /// next entry to read from.
    fn follow_next_in_chain(&mut self, table: &JoinHashTable) {
        for &ent_idx in &self.selection {
            let ent = &mut self.entries[ent_idx];
            // SAFETY: ...
            //
            // Assumes that we're generating row addresses correctly and that
            // they are in bounds.
            *ent = unsafe { table.read_next_entry(ent.row_address()) };
        }

        // Update the selection to only include entries that are not dangling.
        self.selection.clear();
        self.selection
            .extend(self.entries.iter().enumerate().filter_map(|(idx, ent)| {
                if ent.is_dangling() {
                    None
                } else {
                    Some(idx)
                }
            }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::testutil::generate_batch;

    #[test]
    fn directory_as_atomic_entries() {
        // Sanity check with Miri.

        let dir = Directory::new_for_num_rows(16).unwrap();
        dir.get_entry_as_atomic_u64(3).store(
            HashTableEntry::new(
                RowAddress {
                    chunk_idx: 4,
                    row_idx: 3,
                },
                0xABCD_FFFF_FFFF_FFFF,
            )
            .as_u64(),
            atomic::Ordering::Relaxed,
        );

        let expected = HashTableEntry {
            chunk_idx: 4,
            row_idx: 3,
            hash_prefix: 0xABCD,
        };
        let got = dir.get_entry(3);
        assert_eq!(expected, got)
    }

    #[test]
    fn insert_key_only() {
        let mut table = JoinHashTable::new(JoinType::Inner, [DataType::Int32], [0], [], 16);
        let mut build_state = table.init_build_state();

        let input = generate_batch!([1, 2, 3, 4]);
        table.collect_build(&mut build_state, &input).unwrap();

        table.init_directory().unwrap();
        table.insert_hashes_for_chunks([0]).unwrap();
    }
}
