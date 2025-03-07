use std::borrow::Borrow;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalU64, ScalarStorage};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_collection::{AggregateAppendState, AggregateCollection};
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::arrays::row::row_matcher::PredicateRowMatcher;
use crate::arrays::row::row_scan::RowScanState;
use crate::buffer::buffer_manager::NopBufferManager;
use crate::buffer::typed::TypedBuffer;
use crate::execution::operators::util::power_of_two::{
    compute_offset_from_hash,
    inc_and_wrap_offset,
    is_power_of_2,
};
use crate::expr::comparison_expr::ComparisonOperator;

#[derive(Debug)]
pub struct AggregateHashTableInsertState {
    /// Reusable buffer containing pointers to the beginning of rows that we
    /// should update.
    row_ptrs: Vec<*mut u8>,
    /// State for appending to the aggregate collection.
    append_state: AggregateAppendState,
}

// SAFETY: The `Vec<*mut u8>` is just a buffer for storing row pointers.
unsafe impl Send for AggregateHashTableInsertState {}
unsafe impl Sync for AggregateHashTableInsertState {}

/// Linear probing hash table for aggregates.
///
/// Hash table layout: [groups, groups_hash, agg_states]
///
/// - `groups`: The grouping columns.
/// - `groups_hash`: Hash of the grouping columns.
/// - `agg_states`: Aggregate states (aligned).
#[derive(Debug)]
pub struct AggregateHashTable {
    /// Layout to use for storing the aggregate states and group values.
    pub(crate) layout: AggregateLayout,
    /// Hash table directory.
    pub(crate) directory: Directory,
    /// Byte offset to where a hash is stored in a row.
    pub(crate) hash_offset: usize,
    /// Hash table data storing the group and aggregate states.
    pub(crate) data: AggregateCollection,
    /// Group value matcher.
    pub(crate) matcher: GroupMatcher,
    pub(crate) row_capacity: usize,
}

impl AggregateHashTable {
    /// Create a new hash table with the given layout.
    ///
    /// It's expected that the last group column is u64 for storing the hash
    /// value for the groups.
    pub fn try_new(layout: AggregateLayout, row_capacity: usize) -> Result<Self> {
        if layout.groups.num_columns() == 0 {
            return Err(RayexecError::new(
                "Cannot create aggregate hash table with zero groups",
            ));
        }

        if layout.groups.types.last().unwrap() != &DataType::UInt64 {
            return Err(RayexecError::new(
                "Last group type not u64, expected u64 for the hash value",
            ));
        }

        // Hash is the last column in the groups layout.
        let hash_offset = *layout.groups.offsets.last().unwrap();

        let data = AggregateCollection::new(layout.clone(), row_capacity);
        let directory = Directory::try_new(Directory::DEFAULT_CAPACITY)?;
        let matcher = GroupMatcher::new(&layout);

        Ok(AggregateHashTable {
            layout,
            directory,
            hash_offset,
            data,
            matcher,
            row_capacity,
        })
    }

    pub fn init_insert_state(&self) -> AggregateHashTableInsertState {
        AggregateHashTableInsertState {
            row_ptrs: Vec::new(),
            append_state: self.data.init_append_state(),
        }
    }

    /// Insert groups and aggregate inputs into the hash table.
    ///
    /// `inputs` should be ordered inputs to the aggregates in this table.
    ///
    /// See `AggregateLayout::update_states` for specifics.
    pub fn insert(
        &mut self,
        state: &mut AggregateHashTableInsertState,
        groups: &Batch,
        inputs: &Batch,
    ) -> Result<()> {
        // Hash the groups.
        // TODO: Avoid allocating here.
        let mut hashes_arr = Array::new(&NopBufferManager, DataType::UInt64, groups.num_rows)?;
        let hashes = PhysicalU64::get_addressable_mut(&mut hashes_arr.data)?.slice;
        hash_many_arrays(&groups.arrays, 0..groups.num_rows, hashes)?;

        self.insert_with_hashes(state, groups, inputs, &hashes_arr)
    }

    /// Insert groups into the table with precomputed hash values.
    pub fn insert_with_hashes(
        &mut self,
        state: &mut AggregateHashTableInsertState,
        groups: &Batch,
        inputs: &Batch,
        hashes: &Array,
    ) -> Result<()> {
        debug_assert!(hashes.logical_len() >= groups.num_rows);
        debug_assert_eq!(groups.num_rows, inputs.num_rows);

        state.row_ptrs.clear();
        state
            .row_ptrs
            .extend(std::iter::repeat(std::ptr::null_mut()).take(groups.num_rows));

        self.find_or_create_groups(
            &mut state.append_state,
            &groups.arrays,
            hashes,
            groups.num_rows,
            &mut state.row_ptrs,
        )?;

        debug_assert!(
            !state.row_ptrs.iter().any(|ptr| ptr.is_null()),
            "Null pointer at position: {}",
            state.row_ptrs.iter().position(|ptr| ptr.is_null()).unwrap(),
        );

        unsafe {
            self.layout.update_states(
                state.row_ptrs.as_mut_slice(),
                &inputs.arrays,
                inputs.num_rows,
            )?;
        }

        Ok(())
    }

    /// Find or create groups in the hash table.
    ///
    /// This will fill `out_ptrs` with the pointers to use for each row to
    /// update the aggregate state.
    fn find_or_create_groups<A>(
        &mut self,
        append_state: &mut AggregateAppendState,
        groups: &[A],
        hashes_arr: &Array,
        num_rows: usize,
        out_ptrs: &mut [*mut u8],
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        if num_rows == 0 {
            return Ok(());
        }

        debug_assert_eq!(num_rows, out_ptrs.len());
        let hashes = PhysicalU64::get_addressable(&hashes_arr.data)?.slice;
        let hashes = &hashes[0..num_rows];

        if self.directory.needs_resize(num_rows) {
            let new_cap = usize::max(self.directory.capacity() * 2, num_rows);
            self.directory.resize(new_cap)?;
        }

        // Precompute offsets into the table.
        let mut offsets = vec![0; num_rows];
        let cap = self.directory.capacity() as u64;
        for (idx, &hash) in hashes.iter().enumerate() {
            offsets[idx] = compute_offset_from_hash(hash, cap) as usize;
        }

        // Init selection to all rows in input.
        let mut needs_insert: Vec<_> = (0..num_rows).collect();

        // Track rows that require allocating new rows for.
        let mut new_groups = Vec::new();

        // Total number of new groups we created.
        let mut total_new_groups = 0;

        // Track rows that need to be compared to rows already in the table.
        let mut needs_compare = Vec::new();

        // Groups + hashes used when appending new groups to the collection.
        let mut groups_and_hashes = Vec::with_capacity(groups.len() + 1);
        groups_and_hashes.extend(groups.iter().map(|arr| arr.borrow()));
        groups_and_hashes.push(hashes_arr);

        let cap = self.directory.capacity();
        let entries = self.directory.entries.as_slice_mut();

        while needs_insert.len() > 0 {
            new_groups.clear();

            for &row_idx in needs_insert.iter() {
                let offset = &mut offsets[row_idx];
                let hash = hashes[row_idx];

                // Probe.
                //
                // This will update `offsets` as needed. `offsets` then can be
                // used to index directly into entries to point to the correct
                // entry for a row.
                for iter_count in 0..cap {
                    let ent = &mut entries[*offset];

                    if !ent.is_occupied() {
                        // Empty entry, claim it.
                        //
                        // Note that a real row pointer will be added to the
                        // entry later in the function. This just uses a
                        // dangling pointer to indicated occupied.
                        //
                        // This does store the hash so that we can compare rows
                        // if needed.
                        *ent = Entry::new_claimed(hash);
                        new_groups.push(row_idx);

                        break;
                    }

                    if ent.hash == hash {
                        needs_compare.push(row_idx);
                        break;
                    }

                    // Otherwise need to increment.
                    *offset = inc_and_wrap_offset(*offset, cap);

                    if iter_count == cap {
                        // We wrapped. This shouldn't happen during normal
                        // execution as the hash table should've been resized to
                        // fit everything.
                        //
                        // But Sean writes bugs, so just in case...
                        return Err(RayexecError::new("Hash table completely full"));
                    }
                }
            }

            // If we've inserted new group hashes, go ahead and create the
            // actual groups.
            if !new_groups.is_empty() {
                self.data.append_groups(
                    append_state,
                    &groups_and_hashes,
                    new_groups.iter().copied(),
                )?;

                let row_ptrs = append_state.row_pointers();
                debug_assert!(!row_ptrs.iter().any(|p| p.is_null()));
                debug_assert_eq!(new_groups.len(), row_ptrs.len());

                // Update the entries with the new row pointers.
                for (&row_idx, &row_ptr) in new_groups.iter().zip(row_ptrs) {
                    // Offsets updated during probing...
                    let offset = offsets[row_idx];
                    let ent = &mut entries[offset];
                    ent.row_ptr = row_ptr;

                    // Update output pointers.
                    out_ptrs[row_idx] = row_ptr;
                }

                total_new_groups += new_groups.len();
            }

            // If we had hashes match, check the actual values.
            if !needs_compare.is_empty() {
                for &row_idx in &needs_compare {
                    // Offset updated during probing...
                    let offset = offsets[row_idx];
                    let row_ptr = entries[offset].row_ptr;
                    // Set output pointer assuming it will match. If it doesn't,
                    // the following loop iteration(s) will overwrite it,
                    // eventually setting it the correct pointer.
                    out_ptrs[row_idx] = row_ptr;
                }

                needs_insert.clear();
                let _ = self.matcher.find_matches(
                    &self.layout,
                    out_ptrs,
                    &groups_and_hashes,
                    &mut needs_compare,
                    &mut needs_insert,
                )?;

                // For everything that didn't match, increment its offset so the
                // next iteration tries a new slot.
                for &not_match_row_idx in &needs_insert {
                    let offset = &mut offsets[not_match_row_idx];
                    *offset = inc_and_wrap_offset(*offset, cap);
                }
            } else {
                // Otherwise everything either matched with existing groups, or
                // we created new groups.
                //
                // Clear the needs_insert vec to stop the loop.
                needs_insert.clear();
            }
        }

        self.directory.num_occupied += total_new_groups;

        Ok(())
    }

    pub fn merge_from(
        &mut self,
        state: &mut AggregateHashTableInsertState,
        other: &mut Self,
    ) -> Result<()> {
        if self.directory.num_occupied == 0 {
            std::mem::swap(self, other);
            return Ok(());
        }
        if other.directory.num_occupied == 0 {
            return Ok(());
        }

        let mut merge_state =
            MergeScanState::try_new(&other.data, &self.layout, self.row_capacity)?;

        loop {
            merge_state.scan()?;
            if merge_state.groups.num_rows() == 0 {
                // No more groups scanned, we're done.
                break;
            }

            // Append groups we haven't seen before and get dest group pointers.
            let (groups, hashes) = merge_state.split_groups_and_hashes();
            let num_rows = merge_state.groups.num_rows();
            let row_ptrs = &mut state.row_ptrs[0..num_rows];

            self.find_or_create_groups(
                &mut state.append_state,
                groups,
                hashes,
                num_rows,
                row_ptrs,
            )?;

            // Combine states.
            unsafe {
                self.layout
                    .combine_states(merge_state.group_scan.scanned_row_pointers_mut(), row_ptrs)?;
            }
        }

        Ok(())
    }
}

/// State for scanning and merging hash tables.
#[derive(Debug)]
struct MergeScanState<'a> {
    /// Scan state for scanning groups from the source hash table.
    group_scan: RowScanState,
    /// Scanned groups, last array contains the hashes.
    groups: Batch,
    /// Data from the source hash table.
    data: &'a AggregateCollection,
}

impl<'a> MergeScanState<'a> {
    fn try_new(
        data: &'a AggregateCollection,
        layout: &AggregateLayout,
        row_cap: usize,
    ) -> Result<Self> {
        let groups = Batch::new(layout.groups.types.clone(), row_cap)?;
        let group_scan = RowScanState::new_full_scan(data.row_blocks());

        Ok(MergeScanState {
            group_scan,
            groups,
            data,
        })
    }

    /// Scans the next set of groups.
    fn scan(&mut self) -> Result<()> {
        self.groups.reset_for_write()?;

        let capacity = self.groups.write_capacity()?;
        let count =
            self.data
                .scan_groups(&mut self.group_scan, &mut self.groups.arrays, capacity)?;
        self.groups.set_num_rows(count)?;

        Ok(())
    }

    fn split_groups_and_hashes(&self) -> (&[Array], &Array) {
        let len = self.groups.arrays.len();
        let groups = &self.groups.arrays[0..len - 1];
        let hashes = &self.groups.arrays[len - 1];
        (groups, hashes)
    }
}

/// Wrapper around a predicate row matcher for matching on group values.
#[derive(Debug)]
pub(crate) struct GroupMatcher {
    /// Column indices we're matching on the left (should contain all indices).
    // TODO: Skip hash column?
    lhs_columns: Vec<usize>,
    /// The row matcher.
    matcher: PredicateRowMatcher,
}

impl GroupMatcher {
    fn new(layout: &AggregateLayout) -> Self {
        let lhs_columns: Vec<_> = (0..layout.groups.num_columns()).collect();
        let matchers = layout.groups.types.iter().map(|datatype| {
            (
                datatype.physical_type(),
                ComparisonOperator::IsNotDistinctFrom,
            )
        });
        let matcher = PredicateRowMatcher::new(matchers);

        GroupMatcher {
            lhs_columns,
            matcher,
        }
    }

    fn find_matches<A>(
        &self,
        layout: &AggregateLayout,
        lhs_rows: &[*mut u8],
        rhs_columns: &[A],
        selection: &mut Vec<usize>,
        not_matched: &mut Vec<usize>,
    ) -> Result<usize>
    where
        A: Borrow<Array>,
    {
        debug_assert_eq!(self.lhs_columns.len(), rhs_columns.len());

        // SAFETY: Just converting const ptrs to mut ptrs.
        let lhs_rows =
            unsafe { std::slice::from_raw_parts(lhs_rows.as_ptr() as _, lhs_rows.len()) };

        self.matcher.find_matches(
            &layout.groups,
            lhs_rows,
            &self.lhs_columns,
            rhs_columns,
            selection,
            not_matched,
        )
    }
}

/// An entry in the hash table.
#[derive(Debug, Clone, Copy)]
struct Entry {
    /// The hash associated with the entry.
    hash: u64,
    /// Pointer to the start of the row.
    row_ptr: *mut u8,
}

impl Entry {
    const EMPTY: Entry = Entry {
        hash: 0,
        row_ptr: std::ptr::null_mut(),
    };

    const fn is_occupied(&self) -> bool {
        !self.row_ptr.is_null()
    }

    /// Create a new entry that "claims" a slot.
    ///
    /// This will be initialized with a dangling pointer (non-null) such that
    /// further linear probes will see this as occupied.
    ///
    /// The row pointer is expected to be set to the appropriate row after all
    /// probing is done for a new set of rows.
    const fn new_claimed(hash: u64) -> Self {
        Entry {
            hash,
            row_ptr: std::ptr::dangling_mut(),
        }
    }
}

/// Hash table directory containing pointers to rows.
#[derive(Debug)]
pub(crate) struct Directory {
    /// Number of non-null pointers in entries.
    num_occupied: usize,
    /// Row pointers.
    entries: TypedBuffer<Entry>,
}

impl Directory {
    const LOAD_FACTOR: f64 = 0.7;
    const DEFAULT_CAPACITY: usize = 128;

    fn try_new(capacity: usize) -> Result<Self> {
        let capacity = capacity.next_power_of_two();

        let mut entries = TypedBuffer::try_with_capacity(&NopBufferManager, capacity)?;
        // Initialize...
        entries.as_slice_mut().fill(Entry::EMPTY);

        Ok(Directory {
            num_occupied: 0,
            entries,
        })
    }

    fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Resizes the directory to at least `new_capacity`.
    ///
    /// This will ensure the new capacity of the directory is a power of two.
    fn resize(&mut self, mut new_capacity: usize) -> Result<()> {
        if !is_power_of_2(new_capacity) {
            new_capacity = new_capacity.next_power_of_two();
        }
        if new_capacity < self.entries.capacity() {
            return Err(RayexecError::new("Cannot reduce capacity of hash table")
                .with_field("current", self.entries.capacity())
                .with_field("new", new_capacity));
        }

        let old_entries = std::mem::replace(
            &mut self.entries,
            TypedBuffer::try_with_capacity(&NopBufferManager, new_capacity)?,
        );

        let entries = self.entries.as_slice_mut();
        entries.fill(Entry::EMPTY);

        for old_ent in old_entries.as_slice() {
            if !old_ent.is_occupied() {
                continue;
            }

            let mut offset = compute_offset_from_hash(old_ent.hash, new_capacity as u64) as usize;

            // Continue to try to insert until we find an empty slot.
            loop {
                if !entries[offset].is_occupied() {
                    // Empty slot, insert entry.
                    entries[offset] = *old_ent;
                    break;
                }

                // Keep probing.
                offset = inc_and_wrap_offset(offset, new_capacity);
            }
        }

        Ok(())
    }

    /// Returns if the directory needs to be resized to accomadate new inputs.
    fn needs_resize(&self, num_inputs: usize) -> bool {
        (num_inputs + self.num_occupied) >= self.resize_threshold()
    }

    fn resize_threshold(&self) -> usize {
        (self.capacity() as f64 * Self::LOAD_FACTOR) as usize
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::expr::physical::PhysicalAggregateExpression;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
    use crate::testutil::arrays::{assert_arrays_eq, assert_batches_eq, generate_batch};

    /// Helper to get the groups and results from a hash table.
    fn get_groups_and_results(table: &AggregateHashTable) -> (Batch, Batch) {
        let cap = table.data.num_groups();
        let mut out_groups = Batch::new(table.layout.groups.types.clone(), cap).unwrap();
        let mut out_results = Batch::new(
            table
                .layout
                .aggregates
                .iter()
                .map(|agg| agg.function.state.return_type.clone()),
            cap,
        )
        .unwrap();

        let mut row_ptrs: Vec<_> = table.data.row_mut_ptr_iter().collect();
        assert_eq!(row_ptrs.len(), cap);

        unsafe {
            table
                .data
                .finalize_groups(
                    &mut row_ptrs,
                    &mut out_groups.arrays,
                    &mut out_results.arrays,
                )
                .unwrap();
        }

        out_groups.set_num_rows(cap).unwrap();
        out_results.set_num_rows(cap).unwrap();

        (out_groups, out_results)
    }

    #[test]
    fn single_insert_single_agg_multiple_group_vals() {
        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::Int64)],
        )];

        let layout = AggregateLayout::new([DataType::Utf8, DataType::UInt64], aggs);

        let mut table = AggregateHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);

        table.insert(&mut state, &groups, &inputs).unwrap();

        let (out_groups, out_results) = get_groups_and_results(&table);

        let expected_groups = Array::try_from_iter(["group_a", "group_b", "group_c"]).unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!([4_i64, 2, 4]);
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn multiple_inserts_single_agg_multiple_group_vals() {
        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::Int64)],
        )];

        let layout = AggregateLayout::new([DataType::Utf8, DataType::UInt64], aggs);

        let mut table = AggregateHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        table.insert(&mut state, &groups, &inputs).unwrap();

        let groups = generate_batch!(["group_c", "group_d", "group_a", "group_a"]);
        let inputs = generate_batch!([5_i64, 6, 7, 8]);
        table.insert(&mut state, &groups, &inputs).unwrap();

        let (out_groups, out_results) = get_groups_and_results(&table);

        let expected_groups =
            Array::try_from_iter(["group_a", "group_b", "group_c", "group_d"]).unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!([19_i64, 2, 9, 6]);
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn multiple_inserts_different_input_sizes() {
        // Same as above, but send input has fewer rows than the first.

        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::Int64)],
        )];

        let layout = AggregateLayout::new([DataType::Utf8, DataType::UInt64], aggs);

        let mut table = AggregateHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        table.insert(&mut state, &groups, &inputs).unwrap();

        let groups = generate_batch!(["group_c", "group_d"]);
        let inputs = generate_batch!([5_i64, 6]);
        table.insert(&mut state, &groups, &inputs).unwrap();

        let (out_groups, out_results) = get_groups_and_results(&table);

        let expected_groups =
            Array::try_from_iter(["group_a", "group_b", "group_c", "group_d"]).unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!([4_i64, 2, 9, 6]);
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn multiple_inserts_unseen_groups() {
        // Second inputs contains only unseen groups.

        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::Int64)],
        )];

        let layout = AggregateLayout::new([DataType::Utf8, DataType::UInt64], aggs);

        let mut table = AggregateHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        table.insert(&mut state, &groups, &inputs).unwrap();

        let groups = generate_batch!(["group_d", "group_d", "group_e", "group_f"]);
        let inputs = generate_batch!([5_i64, 6, 7, 8]);
        table.insert(&mut state, &groups, &inputs).unwrap();

        let (out_groups, out_results) = get_groups_and_results(&table);

        let expected_groups = Array::try_from_iter([
            "group_a", "group_b", "group_c", "group_d", "group_e", "group_f",
        ])
        .unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!([4_i64, 2, 4, 11, 7, 8]);
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn insert_with_hash_collision() {
        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::Int64)],
        )];

        let layout = AggregateLayout::new([DataType::Utf8, DataType::UInt64], aggs);

        let mut table = AggregateHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        let hashes = Array::try_from_iter([1_u64, 1, 1, 1]).unwrap(); // All groups hashing to the same value
        table
            .insert_with_hashes(&mut state, &groups, &inputs, &hashes)
            .unwrap();

        let (out_groups, out_results) = get_groups_and_results(&table);

        let expected_groups = Array::try_from_iter(["group_a", "group_b", "group_c"]).unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!([4_i64, 2, 4]);
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn merge_tables() {
        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::Int64)],
        )];

        let layout = AggregateLayout::new([DataType::Utf8, DataType::UInt64], aggs);

        let mut t1 = AggregateHashTable::try_new(layout.clone(), 16).unwrap();
        let mut s1 = t1.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        t1.insert(&mut s1, &groups, &inputs).unwrap();

        let mut t2 = AggregateHashTable::try_new(layout.clone(), 16).unwrap();
        let mut s2 = t2.init_insert_state();

        let groups = generate_batch!(["group_c", "group_d", "group_a", "group_a"]);
        let inputs = generate_batch!([5_i64, 6, 7, 8]);
        t2.insert(&mut s2, &groups, &inputs).unwrap();

        t1.merge_from(&mut s2, &mut t2).unwrap();
        assert_eq!(1, t1.data.num_row_blocks());

        let (out_groups, out_results) = get_groups_and_results(&t1);

        let expected_groups =
            Array::try_from_iter(["group_a", "group_b", "group_c", "group_d"]).unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!([19_i64, 2, 9, 6]);
        assert_batches_eq(&expected_results, &out_results);
    }
}
