pub mod scan;

mod directory;

use std::sync::atomic::{self, AtomicBool, AtomicPtr, AtomicUsize};

use directory::Directory;
use glaredb_error::Result;
use scan::HashTablePartitionScanState;

use super::HashJoinCondition;
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
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::logical::logical_join::JoinType;
use crate::util::cell::{UnsafeSyncCell, UnsafeSyncOnceCell};

/// State provided during hash table build.
#[derive(Debug)]
pub struct HashTableBuildPartitionState {
    /// Index of the partition.
    pub partition_idx: usize,
    /// Constant initial values to use for left/outer joins for a "match".
    ///
    /// When we insert into the hash table, we'll use these values (all false)
    /// to initialize matches.
    match_init: Array,
    /// State for appending rows to the collection.
    row_append: RowAppendState,
    /// Evaluating for producing join keys for the build side.
    join_keys_evaluator: ExpressionEvaluator,
    /// Batch for holding the keys we're joining on.
    join_keys: Batch,
}

#[derive(Debug)]
pub struct HashTableOperatorState {
    /// Row collections per partition.
    ///
    /// Initialized when we create the build-side partition states.
    partitioned_row_collection: UnsafeSyncOnceCell<PartitionedRowCollecton>,
    /// The merged row collection consists of all blocks from the partitioned
    /// row collections.
    ///
    /// Once the build side completes, a single partitions will be responsible
    /// for moving the blocks from the partitioned collection into this
    /// collection (and also create the directory).
    merged_row_collection: UnsafeSyncCell<RowCollection>,
    /// Directory containing the hashes and row pointers.
    ///
    /// Intialized by a single partition at the same time we merge the
    /// collections.
    pub(super) directory: UnsafeSyncOnceCell<Directory>,
}

#[derive(Debug)]
struct PartitionedRowCollecton {
    /// Remaining number of partitions still pushing to their collections.
    ///
    /// Used for fencing.
    remaining: AtomicUsize,
    /// Row collections for each partition, indexed by partition idx.
    ///
    /// During the build phase, each partition will insert to its own row
    /// collection.
    collections: Vec<UnsafeSyncCell<RowCollection>>,
}

impl HashTableOperatorState {
    fn partition_count(&self) -> usize {
        self.partitioned_row_collection().collections.len()
    }

    fn partitioned_row_collection(&self) -> &PartitionedRowCollecton {
        unsafe { self.partitioned_row_collection.get().unwrap() }
    }
}

/// Chained hash table for joins.
///
/// Build side layout: [columns, keys, hash/next_entry, matches]
///
/// - 'columns': The columns from the build side batches. Retains the same order
///   they were provided in.
///   'keys': The computed join keys.
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
    /// Byte offset into a row for where the hash/next entry value is stored.
    ///
    /// Precomputed from layout.
    pub build_hash_byte_offset: usize,
    /// Batch size to use for row blocks.
    pub batch_size: usize,
    /// Matcher for evaluating predicates on the rows.
    pub row_matcher: PredicateRowMatcher,
    /// Layout for the build side.
    ///
    /// Includes both the original columns as well as any computed join keys.
    pub layout: RowLayout,
    /// Number of columns in the original left-side input. Used to slice of the
    /// extra join keys during scans.
    pub data_column_count: usize,
    /// Column indices in the encoded row layout that are part of the keys being
    /// compared.
    pub encoded_key_columns: Vec<usize>,
    /// Column indices **relative to the join keys** that are being compared by
    /// equality.
    pub equality_columns: Vec<usize>,
    /// Expressions for producing build side keys.
    pub build_side_exprs: Vec<PhysicalScalarExpression>,
    /// Expressions for producing probe side keys.
    pub probe_side_exprs: Vec<PhysicalScalarExpression>,
}

impl JoinHashTable {
    pub fn try_new(
        join_type: JoinType,
        left_datatypes: impl IntoIterator<Item = DataType>,
        _right_datatypes: impl IntoIterator<Item = DataType>,
        conditions: impl IntoIterator<Item = HashJoinCondition>,
        batch_size: usize,
    ) -> Result<Self> {
        let left_datatypes: Vec<_> = left_datatypes.into_iter().collect();
        let data_column_count = left_datatypes.len();

        // Exprs for build/probe side that produce the join keys.
        let mut build_side_exprs = Vec::new();
        let mut probe_side_exprs = Vec::new();

        let mut matcher_conditions = Vec::new();

        let mut encoded_types = left_datatypes;
        let mut encoded_key_columns = Vec::new();
        let mut equality_columns = Vec::new();

        // TODO: This will currently double-encode columns. We should instead
        // allow picking columns from the compute join keys or the original
        // columns when matching rows.
        for condition in conditions {
            let datatype = condition.left.datatype();
            let phys_type = datatype.physical_type()?;
            debug_assert_eq!(
                phys_type,
                condition.right.datatype().physical_type().unwrap()
            );

            build_side_exprs.push(condition.left);
            probe_side_exprs.push(condition.right);

            let encode_idx = encoded_types.len();
            encoded_types.push(datatype);
            encoded_key_columns.push(encode_idx);

            if condition.op == ComparisonOperator::Eq {
                let equality_idx = equality_columns.len();
                equality_columns.push(equality_idx);
            }

            matcher_conditions.push((phys_type, condition.op));
        }

        debug_assert!(!build_side_exprs.is_empty());
        debug_assert!(!probe_side_exprs.is_empty());
        debug_assert!(
            !equality_columns.is_empty(),
            "Missing equality for join hash table"
        );

        let row_matcher = PredicateRowMatcher::new(matcher_conditions);

        // Add hash to build types.
        let hash_col = encoded_types.len();
        encoded_types.push(DataType::uint64());

        if join_type.produce_all_build_side_rows() {
            // Add 'matches' column, we're dealing with LEFT/OUTER join.
            encoded_types.push(DataType::boolean());
        }

        let layout = RowLayout::try_new(encoded_types)?;
        let build_hash_byte_offset = layout.offsets[hash_col];

        Ok(JoinHashTable {
            join_type,
            build_hash_byte_offset,
            batch_size,
            row_matcher,
            layout,
            data_column_count,
            encoded_key_columns,
            equality_columns,
            build_side_exprs,
            probe_side_exprs,
        })
    }

    /// Create the operator state that gets shared with all partitions, build
    /// and probe side.
    pub fn create_operator_state(&self) -> Result<HashTableOperatorState> {
        let state = HashTableOperatorState {
            partitioned_row_collection: UnsafeSyncOnceCell::new(),
            merged_row_collection: UnsafeSyncCell::new(RowCollection::new(
                self.layout.clone(),
                self.batch_size,
            )),
            directory: UnsafeSyncOnceCell::new(),
        };

        Ok(state)
    }

    /// Create the partition states for the build side of the join.
    pub fn create_build_partition_states(
        &self,
        op_state: &HashTableOperatorState,
        partitions: usize,
    ) -> Result<Vec<HashTableBuildPartitionState>> {
        // Init partitioned row collection.
        unsafe {
            op_state
                .partitioned_row_collection
                .set(PartitionedRowCollecton {
                    remaining: AtomicUsize::new(partitions),
                    collections: (0..partitions)
                        .map(|_| {
                            UnsafeSyncCell::new(RowCollection::new(
                                self.layout.clone(),
                                self.batch_size,
                            ))
                        })
                        .collect(),
                })
                .expect("partitioned row collection to be initialized once")
        };

        // Init partition states.
        let states = op_state
            .partitioned_row_collection()
            .collections
            .iter()
            .enumerate()
            .map(|(partition_idx, collection)| {
                Ok(HashTableBuildPartitionState {
                    partition_idx,
                    match_init: Array::new_constant(
                        &DefaultBufferManager,
                        &false.into(),
                        self.batch_size,
                    )?,
                    join_keys: Batch::new(
                        self.build_side_exprs.iter().map(|expr| expr.datatype()),
                        self.batch_size,
                    )?,
                    join_keys_evaluator: ExpressionEvaluator::try_new(
                        self.build_side_exprs.clone(),
                        self.batch_size,
                    )?,
                    // SAFETY: Only a single thread should be creating the
                    // partition states. There's no other accessess to to these
                    // collections at this point.
                    row_append: unsafe { collection.get() }.init_append(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        debug_assert_eq!(partitions, states.len());

        Ok(states)
    }

    pub fn create_probe_partition_states(
        &self,
        _op_state: &HashTableOperatorState,
        partitions: usize,
    ) -> Result<Vec<HashTablePartitionScanState>> {
        let states = (0..partitions)
            .map(|partition_idx| {
                Ok(HashTablePartitionScanState {
                    partition_idx,
                    selection: Vec::new(),
                    not_matched: Vec::new(),
                    right_matches: Vec::new(),
                    row_pointers: Vec::new(),
                    hashes: Vec::new(),
                    block_read: BlockScanState::empty(),
                    join_keys: Batch::new(
                        self.probe_side_exprs.iter().map(|expr| expr.datatype()),
                        self.batch_size,
                    )?,
                    join_keys_evaluator: ExpressionEvaluator::try_new(
                        self.probe_side_exprs.clone(),
                        self.batch_size,
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(states)
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
        input: &mut Batch,
    ) -> Result<()> {
        // Generate build-side keys.
        state
            .join_keys_evaluator
            .eval_batch(input, input.selection(), &mut state.join_keys)?;

        let mut build_inputs: Vec<&Array> = Vec::with_capacity(
            input.num_arrays() + state.join_keys.num_arrays() + self.extra_column_count(),
        );

        // [columns, join keys]
        build_inputs.extend(input.arrays.iter().chain(&state.join_keys.arrays));

        // Get hash inputs.
        let hash_inputs = self.equality_columns.iter().map(|&idx| {
            // Equality index is relative to the keys.
            let idx = self.encoded_key_columns[idx];
            // Build inputs contains all columns we'll be encoding, except for
            // the hashes and matches.
            build_inputs[idx]
        });

        let mut hashes = Array::new(&DefaultBufferManager, DataType::uint64(), input.num_rows())?;
        let hash_vals = PhysicalU64::buffer_downcast_mut(&mut hashes.data)?;
        hash_many_arrays(
            hash_inputs,
            0..input.num_rows(),
            &mut hash_vals.buffer.as_slice_mut()[0..input.num_rows()],
        )?;

        // Add hashes to arrays we'll be encoding.
        build_inputs.push(&hashes);

        // Ensure we include the "matches" initial values.
        if self.join_type.produce_all_build_side_rows() {
            // Resize to match the input rows.
            state.match_init.select(
                &DefaultBufferManager,
                Selection::constant(input.num_rows(), 0),
            )?;

            // And add to the arrays we'll be appending.
            build_inputs.push(&state.match_init);
        }

        // Append to row collection.
        //
        // SAFETY: This is writing the row collection this partition is
        // responsible for. No other partition should be reading/writing it.
        let row_collection = unsafe {
            op_state.partitioned_row_collection().collections[state.partition_idx].get_mut()
        };
        row_collection.append_arrays(&mut state.row_append, &build_inputs, input.num_rows())?;

        Ok(())
    }

    /// Indicates that this partition is finished building.
    ///
    /// Should be called for all partitions, and before we initialized the
    /// directory.
    ///
    /// Returns `true` if this was the last partition to finish, inidicating it
    /// should create the directory.
    pub fn finish_build(
        &self,
        op_state: &HashTableOperatorState,
        _state: &mut HashTableBuildPartitionState,
    ) -> Result<bool> {
        // We're just using this fencing. We'll have a single partition at the
        // end do the directory init and moving everything into a single
        // collection.
        let remaining = &op_state.partitioned_row_collection().remaining;
        let prev = remaining.fetch_sub(1, atomic::Ordering::Release);
        assert_ne!(
            0, prev,
            "Atomic count for remaining partitions must not go negative"
        );

        // Current count is zero.
        Ok(prev == 1)
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
        // Ensure we see everything.
        let remaining = op_state
            .partitioned_row_collection()
            .remaining
            .load(atomic::Ordering::Acquire);
        assert_eq!(
            0, remaining,
            "Atomic count for partitions must be zero before initializing directory"
        );

        // First merge the row collections.
        let op_row_collection = unsafe { op_state.merged_row_collection.get_mut() };

        for row_collection in &op_state.partitioned_row_collection().collections {
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
    ///
    /// # Safety
    ///
    /// May be called concurrently, but only after the directory has been
    /// initialized.
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

    /// Probe the hash table with the given keys.
    ///
    /// The scan state will be updated for scanning.
    pub fn probe(
        &self,
        op_state: &HashTableOperatorState,
        state: &mut HashTablePartitionScanState,
        rhs: &mut Batch,
    ) -> Result<()> {
        // Generate probe keys.
        state
            .join_keys_evaluator
            .eval_batch(rhs, rhs.selection(), &mut state.join_keys)?;

        // Get equality keys from the rhs.
        let hash_input = self.equality_columns.iter().map(|&idx| {
            // Equality index relative to join keys. We can just index directly
            // into the computed join keys.
            &state.join_keys.arrays[idx]
        });

        // Hash keys.
        state.hashes.resize(rhs.num_rows, 0);
        hash_many_arrays(hash_input, 0..rhs.num_rows, &mut state.hashes)?;

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
                // Column index 1 from the left.
                left: PhysicalScalarExpression::Column((1, DataType::int32()).into()),
                // Column index 0 from the right.
                right: PhysicalScalarExpression::Column((0, DataType::int32()).into()),
                op: ComparisonOperator::Eq,
            }],
            16,
        )
        .unwrap();
        let op_state = table.create_operator_state().unwrap();
        let mut build_states = table.create_build_partition_states(&op_state, 1).unwrap();

        let mut input = generate_batch!(["a", "b", "c", "d"], [1, 2, 3, 4]);
        table
            .collect_build(&op_state, &mut build_states[0], &mut input)
            .unwrap();
        let is_last = table.finish_build(&op_state, &mut build_states[0]).unwrap();
        assert!(is_last);

        unsafe { table.init_directory(&op_state).unwrap() };
        unsafe {
            table
                .process_hashes(&op_state, &mut build_states[0])
                .unwrap()
        };

        let mut scan_states = table.create_probe_partition_states(&op_state, 1).unwrap();
        let mut rhs = generate_batch!([2, 3, 5]);
        table
            .probe(&op_state, &mut scan_states[0], &mut rhs)
            .unwrap();

        let mut out =
            Batch::new([DataType::utf8(), DataType::int32(), DataType::int32()], 16).unwrap();
        scan_states[0]
            .scan_next(&table, &op_state, &mut rhs, &mut out)
            .unwrap();

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
                // Column index 1 from the left.
                left: PhysicalScalarExpression::Column((1, DataType::int32()).into()),
                // Column index 0 from the right.
                right: PhysicalScalarExpression::Column((0, DataType::int32()).into()),
                op: ComparisonOperator::Eq,
            }],
            16,
        )
        .unwrap();
        let op_state = table.create_operator_state().unwrap();
        let mut build_states = table.create_build_partition_states(&op_state, 1).unwrap();

        let mut input = generate_batch!(["a", "b", "c", "d"], [1, 2, 3, 3]);
        table
            .collect_build(&op_state, &mut build_states[0], &mut input)
            .unwrap();
        let is_last = table.finish_build(&op_state, &mut build_states[0]).unwrap();
        assert!(is_last);

        unsafe { table.init_directory(&op_state).unwrap() };
        unsafe {
            table
                .process_hashes(&op_state, &mut build_states[0])
                .unwrap()
        };

        let mut scan_states = table.create_probe_partition_states(&op_state, 1).unwrap();
        let mut rhs = generate_batch!([2, 4, 3, 5]);
        table
            .probe(&op_state, &mut scan_states[0], &mut rhs)
            .unwrap();

        let mut out =
            Batch::new([DataType::utf8(), DataType::int32(), DataType::int32()], 16).unwrap();
        scan_states[0]
            .scan_next(&table, &op_state, &mut rhs, &mut out)
            .unwrap();

        let expected = generate_batch!(["b", "d"], [2, 3], [2, 3]);
        assert_batches_eq(&expected, &out);

        // Continue to next, following the chain.
        // TODO: We should modify the scan to try to read up to the capacity of
        // the output batch.
        scan_states[0]
            .scan_next(&table, &op_state, &mut rhs, &mut out)
            .unwrap();

        let expected = generate_batch!(["c"], [3], [3]);
        assert_batches_eq(&expected, &out);
    }
}
