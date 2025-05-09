pub mod scan;

mod directory;

use std::sync::atomic::{self, AtomicBool, AtomicPtr};

use directory::Directory;
use glaredb_error::Result;
use scan::HashTablePartitionScanState;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalU64, ScalarStorage};
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::arrays::row::block_scan::BlockScanState;
use crate::arrays::row::row_collection::{RowAppendState, RowCollection};
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::row::row_matcher::PredicateRowMatcher;
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::logical::logical_join::JoinType;
use crate::util::cell::{UnsafeSyncCell, UnsafeSyncOnceCell};

/// Join condition between left and right batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashJoinCondition {
    /// Index of the column on the left (build) side.
    pub left: usize,
    /// Index of the column on the right (probe) side.
    pub right: usize,
    /// The comparison operator.
    pub op: ComparisonOperator,
}

/// State provided during hash table build.
#[derive(Debug)]
pub struct HashTableBuildPartitionState {
    /// Index of the partition.
    partition_idx: usize,
    /// Initial values to use for left/outer joins for a "match".
    ///
    /// When we insert into the hash table, we'll use these values (all false)
    /// to initialize matches.
    // wtf is this?
    match_init: Array,
    /// State for appending rows to the collection.
    row_append: RowAppendState,
}

#[derive(Debug)]
pub struct HashTableOperatorState {
    // SPEC: Need atomic remaining count for fenching.
    /// Row collections for each partition, indexed by partition idx.
    ///
    /// During the build phase, each partition will insert to its own row
    /// collection.
    pub(super) partitioned_row_collection: Vec<UnsafeSyncCell<RowCollection>>,
    /// The merged row collection consists of all blocks from the partitioned
    /// row collections.
    ///
    /// Once the build side completes, a single partitions will be responsible
    /// for moving the blocks from the partitioned collection into this
    /// collection (and also create the directory).
    pub(super) merged_row_collection: UnsafeSyncCell<RowCollection>,
    /// Directory containing the hashes and row pointers.
    ///
    /// Intialized by a single partition at the same time we merge the
    /// collections.
    pub(super) directory: UnsafeSyncOnceCell<Directory>,
}

impl HashTableOperatorState {
    fn partition_count(&self) -> usize {
        self.partitioned_row_collection.len()
    }
}

/// Chained hash table for joins.
///
/// Build side layout: [columns, hash/next_entry, matches]
///
/// - 'columns': The columns from the build side batches. Retains the same order
///   they were provided in.
/// - 'hash/next_entry': During build, this column stores 64-bit hashes. Once we
///   finalize the hash table and build the directory, this will change to store
///   pointers to the next row in the chain.
/// - 'matches': Optional bool column for tracking rows that matched between the
///   left and right sides. Used for LEFT/OUTER joins.
///
/// Note that the hashes/next_entry column is stored as 64 bits. For
/// systems that use 32 bit pointers (wasm), this should still function
/// correctly. The first 32 bits will be the actual pointer, the trailing
/// 32 bits will be meaningless data (that we don't read).
#[allow(unused)]
#[derive(Debug)]
pub struct JoinHashTable {
    /// Join type this hash table is for.
    pub join_type: JoinType,
    /// Column indices on the input batch for keys on the build side.
    pub build_key_columns: Vec<usize>,
    /// Column indices for all columns on the probe side that need to be
    /// compared (not just equality checked).
    pub build_comparison_columns: Vec<usize>,
    /// Column indices on the input batch for keys on the probe side.
    pub probe_key_columns: Vec<usize>,
    /// Column indices for all columns on the probe side that need to be
    /// compared (not just equality checked).
    pub probe_comparison_columns: Vec<usize>,
    /// Byte offset into a row for where the hash/next entry value is stored.
    ///
    /// Precomputed from layout.
    pub build_hash_byte_offset: usize,
    /// Configured batch size for the join operator.
    pub batch_size: usize,
    /// Matcher for evaluating predicates on the rows.
    pub row_matcher: PredicateRowMatcher,
    pub layout: RowLayout,
}

impl JoinHashTable {
    #[allow(unused)]
    pub fn try_new(
        join_type: JoinType,
        left_datatypes: impl IntoIterator<Item = DataType>,
        right_datatypes: impl IntoIterator<Item = DataType>,
        conditions: impl IntoIterator<Item = HashJoinCondition>,
        batch_size: usize,
    ) -> Result<Self> {
        let mut left_datatypes: Vec<_> = left_datatypes.into_iter().collect();
        let right_datatypes: Vec<_> = right_datatypes.into_iter().collect();

        let mut build_key_columns = Vec::new();
        let mut build_comparison_columns = Vec::new();
        let mut probe_key_columns = Vec::new();
        let mut probe_comparison_columns = Vec::new();
        let mut matcher_conditions = Vec::new();

        for condition in conditions {
            if condition.op == ComparisonOperator::Eq {
                // Add columns as keys.
                build_key_columns.push(condition.left);
                probe_key_columns.push(condition.right);
            }

            let phys_type = left_datatypes[condition.left].physical_type()?;
            debug_assert_eq!(phys_type, right_datatypes[condition.right].physical_type()?);

            matcher_conditions.push((phys_type, condition.op));

            build_comparison_columns.push(condition.left);
            probe_comparison_columns.push(condition.right);
        }

        debug_assert!(!build_key_columns.is_empty());
        debug_assert!(!probe_key_columns.is_empty());

        let row_matcher = PredicateRowMatcher::new(matcher_conditions);

        // Add hash to build types.
        let hash_col = left_datatypes.len();
        left_datatypes.push(DataType::uint64());

        if join_type.produce_all_build_side_rows() {
            // Add 'matches' column, we're dealing with LEFT/OUTER join.
            left_datatypes.push(DataType::boolean());
        }

        let layout = RowLayout::try_new(left_datatypes)?;
        let build_hash_byte_offset = layout.offsets[hash_col];

        Ok(JoinHashTable {
            join_type,
            build_key_columns,
            build_comparison_columns,
            probe_key_columns,
            probe_comparison_columns,
            build_hash_byte_offset,
            batch_size,
            row_matcher,
            layout,
        })
    }

    /// Create the operator state that gets shared with all partitions, build
    /// and probe side.
    pub fn create_operator_state(&self) -> Result<HashTableOperatorState> {
        unimplemented!()
    }

    /// Create the partition states for the build side of the join.
    pub fn create_build_partition_states(
        &self,
        op_state: &HashTableOperatorState,
        partitions: usize,
    ) -> Result<Vec<HashTableBuildPartitionState>> {
        unimplemented!()
    }

    /// Get the column index for the hash in the row collection.
    fn hash_column_idx(&self) -> usize {
        if self.join_type.produce_all_build_side_rows() {
            self.layout.num_columns() - 2 // Hash second to last.
        } else {
            self.layout.num_columns() - 1 // Hash is last column.
        }
    }

    /// Return the number of extra columns on the build side.
    pub const fn extra_column_count(&self) -> usize {
        if self.join_type.produce_all_build_side_rows() {
            2 // Hashes + matches
        } else {
            1 // Hashes
        }
    }

    /// Collects data for the build side of a join.
    ///
    /// This will hash the key columns and insert batches into the row
    /// collection.
    pub fn collect_build(
        &self,
        op_state: &HashTableOperatorState,
        state: &mut HashTableBuildPartitionState,
        input: &Batch,
    ) -> Result<()> {
        let cap = input.arrays.len() + self.extra_column_count();
        let mut build_arrays = Vec::with_capacity(cap);

        // Get build keys from the left for hashing.
        build_arrays.extend(self.build_key_columns.iter().map(|&idx| &input.arrays[idx]));

        let mut hashes = Array::new(&DefaultBufferManager, DataType::uint64(), input.num_rows())?;
        let hash_vals = PhysicalU64::buffer_downcast_mut(&mut hashes.data)?;
        hash_many_arrays(
            build_arrays.iter().copied(),
            0..input.num_rows(),
            &mut hash_vals.buffer.as_slice_mut()[0..input.num_rows()],
        )?;

        // Now just get all build-side arrays.
        build_arrays.clear();
        build_arrays.extend(input.arrays.iter());
        build_arrays.push(&hashes);

        // Ensure we include the "matches" initial values.
        if self.join_type.produce_all_build_side_rows() {
            // Resize to match the input rows.
            state.match_init.select(
                &DefaultBufferManager,
                Selection::constant(input.num_rows(), 0),
            )?;

            // And add to the arrays we'll be appending.
            build_arrays.push(&state.match_init);
        }

        // Append to row collection.
        //
        // SAFETY: This is writing the row collection this partition is
        // responsible for. No other partition should be reading/writing it.
        let row_collection =
            unsafe { op_state.partitioned_row_collection[state.partition_idx].get_mut() };
        row_collection.append_arrays(&mut state.row_append, &build_arrays, input.num_rows())?;

        Ok(())
    }

    /// Initialize the directory for the hash table and move all row blocks into
    /// a single collection.
    ///
    /// # Safety
    ///
    /// This must only be done once and after all build-side data has been
    /// collected.
    ///
    /// We're going to be touching some operator-level variables here.
    pub unsafe fn init_directory(&self, op_state: &HashTableOperatorState) -> Result<()> {
        // First merge the row collections.
        let op_row_collection = unsafe { op_state.merged_row_collection.get_mut() };

        for row_collection in &op_state.partitioned_row_collection {
            let row_collection = unsafe { row_collection.get_mut() };
            op_row_collection.merge_from(row_collection)?;
        }

        // Now init the directory using an exact size. We won't be resizing the
        // directory.
        let row_count = op_row_collection.row_count();

        let directory = Directory::new_for_num_rows(row_count)?;
        unsafe { op_state.directory.set(directory) }
            .expect("directory to be initialized only once");

        Ok(())
    }

    /// Processes hashes for the given blocks into the hash table.
    ///
    /// This should be called after all data has been collected for the build
    /// side, and the directory having been initialized.
    ///
    /// All partitions are expected to call this, and each partition will be
    /// responsible for processing disjoint block indices for insertion into the
    /// hash table.
    pub unsafe fn process_hashes(
        &self,
        op_state: &HashTableOperatorState,
        state: &mut HashTableBuildPartitionState,
    ) -> Result<()> {
        let data = unsafe { op_state.merged_row_collection.get() };
        let num_blocks = data.blocks().num_row_blocks();

        // Ensure we work on dijoint sets of blocks.
        let partition_block_indices =
            (state.partition_idx..num_blocks).step_by(op_state.partition_count());

        let mut hashes = Array::new(&DefaultBufferManager, DataType::uint64(), self.batch_size)?;
        let mut scan_state = data.init_partial_scan(partition_block_indices);

        let scan_cols = &[self.hash_column_idx()];

        let directory =
            unsafe { op_state.directory.get() }.expect("directory to have been initialized");

        loop {
            let count = data.scan_columns(
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

            self.insert_hashes(directory, hashes, scan_state.scanned_row_pointers())?;
        }

        Ok(())
    }

    /// Inserts hashes into the hash table.
    fn insert_hashes(
        &self,
        directory: &Directory,
        hashes: &[u64],
        row_pointers: &[*const u8],
    ) -> Result<()> {
        debug_assert_eq!(hashes.len(), row_pointers.len());

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

    #[allow(unused)]
    pub fn init_scan_state(&self) -> HashTablePartitionScanState {
        HashTablePartitionScanState {
            selection: Vec::new(),
            not_matched: Vec::new(),
            row_pointers: Vec::new(),
            hashes: Vec::new(),
            block_read: BlockScanState::empty(),
        }
    }

    /// Probe the hash table with the given keys.
    ///
    /// The scan state will be updated for scanning.
    pub fn probe(
        &self,
        op_state: &HashTableOperatorState,
        state: &mut HashTablePartitionScanState,
        rhs: &Batch,
    ) -> Result<()> {
        // TODO: Reuse array.
        let keys: Vec<_> = self
            .probe_key_columns
            .iter()
            .map(|&idx| &rhs.arrays[idx])
            .collect();

        // Hash keys.
        state.hashes.resize(rhs.num_rows, 0);
        hash_many_arrays(keys, 0..rhs.num_rows, &mut state.hashes)?;

        // Resize entries to number of keys we're probing with. These will be
        // overwritten in the below loop.
        state.row_pointers.resize(rhs.num_rows, std::ptr::null());

        let directory =
            unsafe { op_state.directory.get() }.expect("directory to have been initialized");

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

        // Initialize selection where pointers are not null.
        state.selection.clear();
        state.selection.extend(
            state
                .row_pointers
                .iter()
                .enumerate()
                .filter_map(|(idx, ptr)| if ptr.is_null() { None } else { Some(idx) }),
        );

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
        atomic_ent
            .compare_exchange(
                std::ptr::null_mut(),
                new_ent.cast_mut(),
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_ok()
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
        unsafe {
            let match_offset = *self.layout.offsets.last().expect("match offset to exist");

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
    }

    unsafe fn write_next_entry_ptr(&self, row_ptr: *const u8, next_ent: *const u8) {
        unsafe {
            let next_ent_ptr = row_ptr
                .byte_add(self.build_hash_byte_offset)
                .cast::<*const u8>()
                .cast_mut();
            next_ent_ptr.write_unaligned(next_ent);
        }
    }

    pub unsafe fn read_next_entry_ptr(&self, row_ptr: *const u8) -> *const u8 {
        unsafe {
            let next_ent_ptr = row_ptr
                .byte_add(self.build_hash_byte_offset)
                .cast::<*const u8>();
            next_ent_ptr.read_unaligned()
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};

    #[test]
    fn inner_join_single_eq_predicate() {
        let table = JoinHashTable::try_new(
            JoinType::Inner,
            [DataType::utf8(), DataType::int32()],
            [DataType::int32()],
            [HashJoinCondition {
                left: 1,
                right: 0,
                op: ComparisonOperator::Eq,
            }],
            16,
        )
        .unwrap();
        let op_state = table.create_operator_state().unwrap();
        let mut build_states = table.create_build_partition_states(&op_state, 1).unwrap();

        let input = generate_batch!(["a", "b", "c", "d"], [1, 2, 3, 4]);
        table
            .collect_build(&op_state, &mut build_states[0], &input)
            .unwrap();

        unsafe { table.init_directory(&op_state).unwrap() };
        unsafe {
            table
                .process_hashes(&op_state, &mut build_states[0])
                .unwrap()
        };

        let mut state = table.init_scan_state();
        let mut rhs = generate_batch!([2, 3, 5]);
        table.probe(&op_state, &mut state, &rhs).unwrap();

        let mut out =
            Batch::new([DataType::utf8(), DataType::int32(), DataType::int32()], 16).unwrap();
        state.scan_next(&table, &mut rhs, &mut out).unwrap();

        let expected = generate_batch!(["b", "c"], [2, 3], [2, 3]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn inner_join_single_eq_predicate_chained() {
        let table = JoinHashTable::try_new(
            JoinType::Inner,
            [DataType::utf8(), DataType::int32()],
            [DataType::int32()],
            [HashJoinCondition {
                left: 1,
                right: 0,
                op: ComparisonOperator::Eq,
            }],
            16,
        )
        .unwrap();
        let op_state = table.create_operator_state().unwrap();
        let mut build_states = table.create_build_partition_states(&op_state, 1).unwrap();

        let input = generate_batch!(["a", "b", "c", "d"], [1, 2, 3, 3]);
        table
            .collect_build(&op_state, &mut build_states[0], &input)
            .unwrap();

        unsafe { table.init_directory(&op_state).unwrap() };
        unsafe {
            table
                .process_hashes(&op_state, &mut build_states[0])
                .unwrap()
        };

        let mut state = table.init_scan_state();
        let mut rhs = generate_batch!([2, 4, 3, 5]);
        table.probe(&op_state, &mut state, &rhs).unwrap();

        let mut out =
            Batch::new([DataType::utf8(), DataType::int32(), DataType::int32()], 16).unwrap();
        state.scan_next(&table, &mut rhs, &mut out).unwrap();

        let expected = generate_batch!(["b", "d"], [2, 3], [2, 3]);
        assert_batches_eq(&expected, &out);

        // Continue to next, following the chain.
        // TODO: We should modify the scan to try to read up to the capacity of
        // the output batch.
        state.scan_next(&table, &mut rhs, &mut out).unwrap();

        let expected = generate_batch!(["c"], [3], [3]);
        assert_batches_eq(&expected, &out);
    }
}
