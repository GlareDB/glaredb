use std::borrow::Borrow;
use std::ptr::NonNull;

use glaredb_error::{DbError, Result};

use super::directory::Directory;
use super::hll::HyperLogLog;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalU64, ScalarStorage};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_collection::{AggregateAppendState, AggregateCollection};
use crate::arrays::row::aggregate_layout::{AggregateLayout, CompleteInputSelector};
use crate::arrays::row::row_matcher::PredicateRowMatcher;
use crate::arrays::row::row_scan::RowScanState;
use crate::execution::operators::hash_aggregate::hash_table::directory::{
    compute_offset_from_hash,
    inc_and_wrap_offset,
};
use crate::expr::comparison_expr::ComparisonOperator;
use crate::util::iter::IntoExactSizeIterator;

/// Hash value to use when not provided with any groups.
///
/// Non-zero for debuggability.
pub const NO_GROUPS_HASH_VALUE: u64 = 49820;

#[derive(Debug)]
pub struct BaseHashTableInsertState {
    /// Reusable buffer containing pointers to the beginning of rows that we
    /// should update.
    row_ptrs: Vec<*mut u8>,
    /// State for appending to the aggregate collection.
    append_state: AggregateAppendState,
    /// Buffers used during insert.
    buffers: InsertBuffers,
}

#[derive(Debug)]
struct InsertBuffers {
    /// Buffer for storing the selected hashes.
    selected_hashes: Vec<u64>,
    /// Precomputed offsets into the table.
    offsets: Vec<usize>,
    /// Selection of rows that need to be inserted into the table.
    needs_insert: Vec<usize>,
    /// Track groups that have never been seen before.
    new_groups: Vec<usize>,
    /// Track rows that we need to compare with existing groups.
    needs_compare: Vec<usize>,
}

// SAFETY: The `Vec<*mut u8>` is just a buffer for storing row pointers to row
// blocks.
unsafe impl Send for BaseHashTableInsertState {}
unsafe impl Sync for BaseHashTableInsertState {}

/// Linear probing hash table for aggregates.
///
/// Hash table layout: [groups, groups_hash, agg_states]
///
/// - `groups`: The grouping columns.
/// - `groups_hash`: Hash of the grouping columns.
/// - `agg_states`: Aggregate states (aligned).
#[derive(Debug)]
pub struct BaseHashTable {
    /// Layout to use for storing the aggregate states and group values.
    pub(crate) layout: AggregateLayout,
    /// Hash table directory.
    pub(crate) directory: Directory,
    /// Hash table data storing the group and aggregate states.
    pub(crate) data: AggregateCollection,
    /// Group value matcher.
    pub(crate) matcher: GroupMatcher,
    pub(crate) row_capacity: usize,
    /// HyperLogLog sketch for estimating the number of distinct values in the
    /// hash table.
    pub(crate) hll: HyperLogLog,
}

impl BaseHashTable {
    /// Create a new hash table with the given layout.
    ///
    /// It's expected that the last group column is u64 for storing the hash
    /// value for the groups.
    pub fn try_new(layout: AggregateLayout, row_capacity: usize) -> Result<Self> {
        if layout.groups.num_columns() == 0 {
            return Err(DbError::new(
                "Cannot create aggregate hash table with zero groups",
            ));
        }

        if layout.groups.types.last().unwrap() != DataType::UINT64 {
            return Err(DbError::new(
                "Last group type not u64, expected u64 for the hash value",
            ));
        }

        let data = AggregateCollection::new(layout.clone(), row_capacity);
        let directory = Directory::try_with_capacity(Directory::DEFAULT_CAPACITY)?;
        let matcher = GroupMatcher::try_new(&layout)?;

        Ok(BaseHashTable {
            layout,
            directory,
            data,
            matcher,
            row_capacity,
            hll: HyperLogLog::new(HyperLogLog::DEFAULT_P),
        })
    }

    pub fn init_insert_state(&self) -> BaseHashTableInsertState {
        BaseHashTableInsertState {
            row_ptrs: Vec::new(),
            append_state: self.data.init_append_state(),
            buffers: InsertBuffers {
                selected_hashes: Vec::new(),
                offsets: Vec::new(),
                needs_insert: Vec::new(),
                new_groups: Vec::new(),
                needs_compare: Vec::new(),
            },
        }
    }

    /// Gets the number of groups in this hash table.
    #[allow(unused)]
    pub fn num_groups(&self) -> usize {
        debug_assert_eq!(self.data.num_groups(), self.directory.num_occupied);
        self.data.num_groups()
    }

    /// Insert groups into the table with precomputed hash values.
    pub fn insert_with_hashes(
        &mut self,
        state: &mut BaseHashTableInsertState,
        agg_selection: &[usize],
        groups: &Batch,
        inputs: &Batch,
        hashes: &Array,
    ) -> Result<()> {
        debug_assert_eq!(hashes.logical_len(), groups.num_rows);
        debug_assert_eq!(groups.num_rows, inputs.num_rows);

        state.row_ptrs.clear();
        state.row_ptrs.resize(groups.num_rows, std::ptr::null_mut());

        self.find_or_create_groups(
            &mut state.buffers,
            &mut state.append_state,
            &groups.arrays,
            hashes,
            groups.num_rows,
            &mut state.row_ptrs,
        )?;

        debug_assert!(
            !state.row_ptrs.iter().any(|ptr| ptr.is_null()),
            "Table Insert: null pointer at position: {}",
            state.row_ptrs.iter().position(|ptr| ptr.is_null()).unwrap(),
        );

        unsafe {
            self.layout.update_states(
                state.row_ptrs.as_mut_slice(),
                CompleteInputSelector::with_selection(&self.layout, agg_selection, &inputs.arrays),
                groups.num_rows,
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
        buffers: &mut InsertBuffers,
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
        // Output hashes into a linear buffer to avoid having to go through the
        // selection in the probe loop.
        let selected_hashes: &[u64] = {
            let hashes_exec = PhysicalU64::downcast_execution_format(&hashes_arr.data)?
                .into_selection_format()?;
            let hashes_buf = hashes_exec.buffer.buffer.as_slice();
            let hashes_sel = hashes_exec.selection;

            buffers.selected_hashes.clear();
            buffers
                .selected_hashes
                .extend(hashes_sel.iter().take(num_rows).map(|sel| hashes_buf[sel]));

            &buffers.selected_hashes
        };
        debug_assert_eq!(num_rows, selected_hashes.len());

        // Update our HLL.
        for &hash in selected_hashes {
            self.hll.insert(hash);
        }

        if self.directory.needs_resize(num_rows) {
            let new_cap = usize::max(
                self.directory.capacity() * 2,
                num_rows + self.directory.capacity(), // Ensure we can at least fit these additional rows.
            );
            self.directory.resize(new_cap)?;

            debug_assert!({
                let rem_cap = self.directory.capacity() - self.directory.num_occupied;
                rem_cap >= num_rows
            })
        }

        // Precompute offsets into the table.
        buffers.offsets.resize(num_rows, 0);
        let offsets = &mut buffers.offsets;
        let cap = self.directory.capacity() as u64;
        for idx in 0..num_rows {
            let hash = selected_hashes[idx];
            offsets[idx] = compute_offset_from_hash(hash, cap) as usize;
        }

        // Init selection to all rows in input.
        buffers.needs_insert.clear();
        buffers.needs_insert.extend(0..num_rows);
        let needs_insert: &mut Vec<_> = &mut buffers.needs_insert;

        // Track rows that require allocating new rows for.
        buffers.new_groups.clear();
        let additional = num_rows.saturating_sub(buffers.new_groups.capacity());
        buffers.new_groups.reserve(additional);
        let new_groups: &mut Vec<_> = &mut buffers.new_groups;

        // Total number of new groups we created.
        let mut total_new_groups = 0;

        // Track rows that need to be compared to rows already in the table.
        buffers.needs_compare.clear();
        let additional = num_rows.saturating_sub(buffers.needs_compare.capacity());
        buffers.needs_compare.reserve(additional);
        let needs_compare: &mut Vec<_> = &mut buffers.needs_compare;

        // Groups + hashes used when appending new groups to the collection.
        let mut groups_and_hashes = Vec::with_capacity(groups.len() + 1);
        groups_and_hashes.extend(groups.iter().map(|arr| arr.borrow()));
        groups_and_hashes.push(hashes_arr);

        let cap = self.directory.capacity();
        let entries = self.directory.entries.as_slice_mut();

        while !needs_insert.is_empty() {
            new_groups.clear();
            needs_compare.clear();

            for &row_idx in needs_insert.iter() {
                let offset = &mut offsets[row_idx];
                let hash = selected_hashes[row_idx];

                // Probe.
                //
                // This will update `offsets` as needed. `offsets` then can be
                // used to index directly into entries to point to the correct
                // entry for a row.
                let mut iter_count = 0;
                while iter_count < cap {
                    let ent = &mut entries[*offset];

                    if ent.ptr.is_none() {
                        // Empty entry, claim it.
                        //
                        // Note that a real row pointer will be added to the
                        // entry later in the function. This just uses a
                        // dangling pointer to indicated occupied.
                        //
                        // This does store the hash so that we can compare rows
                        // if needed.
                        ent.ptr = Some(NonNull::dangling());
                        ent.hash = hash;
                        new_groups.push(row_idx);

                        break;
                    }

                    if ent.hash == hash {
                        needs_compare.push(row_idx);
                        break;
                    }

                    // Otherwise need to increment.
                    *offset = inc_and_wrap_offset(*offset, cap);
                    iter_count += 1;
                }
                if iter_count == cap {
                    // We wrapped. This shouldn't happen during normal
                    // execution as the hash table should've been resized to
                    // fit everything.
                    //
                    // But Sean writes bugs, so just in case...
                    return Err(DbError::new("Hash table completely full").with_field("cap", cap));
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
                debug_assert!(row_ptrs.iter().all(|p| !p.is_null()));
                debug_assert_eq!(new_groups.len(), row_ptrs.len());

                // Update the entries with the new row pointers. Hashes should
                // already be set.
                for (&row_idx, &row_ptr) in new_groups.iter().zip(row_ptrs) {
                    // Offsets updated during probing...
                    let offset = offsets[row_idx];
                    entries[offset].ptr = NonNull::new(row_ptr);

                    // Update output pointers.
                    out_ptrs[row_idx] = row_ptr;
                }

                total_new_groups += new_groups.len();
            }

            // If we had hashes match, check the actual values.
            if !needs_compare.is_empty() {
                for &row_idx in &*needs_compare {
                    // Offset updated during probing...
                    let offset = offsets[row_idx];
                    let row_ptr = entries[offset].ptr;
                    // Set output pointer assuming it will match. If it doesn't,
                    // the following loop iteration(s) will overwrite it,
                    // eventually setting it the correct pointer.
                    out_ptrs[row_idx] = row_ptr.expect("ptr to be Some").as_ptr();
                }

                needs_insert.clear();
                // Matcher updates `needs_insert` with values from
                // `needs_compare` don't match.
                //
                // `needs_insert` should be preserved for the next iteration of
                // the loop, `needs_compare` should be cleared.
                let _ = self.matcher.find_matches(
                    &self.layout,
                    out_ptrs,
                    &groups_and_hashes,
                    needs_compare,
                    needs_insert,
                )?;

                // For everything that didn't match, increment its offset so the
                // next iteration tries a new slot.
                for &not_match_row_idx in &*needs_insert {
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

    /// Merge many hash tables into self.
    pub fn merge_many_into(
        &mut self,
        state: &mut BaseHashTableInsertState,
        agg_selection: impl IntoExactSizeIterator<Item = usize> + Clone,
        tables: &mut [Self],
    ) -> Result<()> {
        // Update our sketch with the sketches from the other hash tables. We'll
        // use this to try to resize to good size.
        for table in &*tables {
            self.hll.merge(&table.hll);
        }

        let est_cardinality = self.hll.count() as usize;
        let est_additional = est_cardinality.saturating_sub(self.directory.num_occupied);

        // Now try to resize based on our estimated cardinality.
        if self.directory.needs_resize(est_additional) {
            self.directory
                .resize(est_additional + self.directory.num_occupied)?;
        }

        // Now do the actual merging.
        for other in tables {
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

                state.row_ptrs.resize(num_rows, std::ptr::null_mut());

                self.find_or_create_groups(
                    &mut state.buffers,
                    &mut state.append_state,
                    groups,
                    hashes,
                    num_rows,
                    &mut state.row_ptrs,
                )?;

                debug_assert!(
                    !state.row_ptrs.iter().any(|ptr| ptr.is_null()),
                    "Table Merge: null pointer at position: {}",
                    state.row_ptrs.iter().position(|ptr| ptr.is_null()).unwrap(),
                );

                // Combine states.
                unsafe {
                    self.layout.combine_states(
                        agg_selection.clone(),
                        merge_state.group_scan.scanned_row_pointers_mut(),
                        &mut state.row_ptrs,
                    )?;
                }
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
    fn try_new(layout: &AggregateLayout) -> Result<Self> {
        let lhs_columns: Vec<_> = (0..layout.groups.num_columns()).collect();
        let matchers = layout
            .groups
            .types
            .iter()
            .map(|datatype| {
                Ok((
                    datatype.physical_type()?,
                    ComparisonOperator::IsNotDistinctFrom,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let matcher = PredicateRowMatcher::new(matchers);

        Ok(GroupMatcher {
            lhs_columns,
            matcher,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::physical_type::MutableScalarStorage;
    use crate::arrays::compute::hash::hash_many_arrays;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::expr::physical::PhysicalAggregateExpression;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
    use crate::generate_array;
    use crate::testutil::arrays::{assert_arrays_eq, assert_batches_eq, generate_batch};
    use crate::util::iter::TryFromExactSizeIterator;

    /// Helper for hashing groups before inserting.
    ///
    /// This will automatically select all rows for aggregating.
    fn hash_and_insert(
        table: &mut BaseHashTable,
        state: &mut BaseHashTableInsertState,
        agg_selection: &[usize],
        groups: &Batch,
        inputs: &Batch,
    ) {
        let mut hashes_arr =
            Array::new(&DefaultBufferManager, DataType::uint64(), groups.num_rows).unwrap();
        let hashes = PhysicalU64::get_addressable_mut(&mut hashes_arr.data)
            .unwrap()
            .slice;
        hash_many_arrays(&groups.arrays, 0..groups.num_rows, hashes).unwrap();

        if groups.arrays.is_empty() {
            hashes.fill(NO_GROUPS_HASH_VALUE);
        }

        table
            .insert_with_hashes(state, agg_selection, groups, inputs, &hashes_arr)
            .unwrap();
    }

    /// Helper to get the groups and results from a hash table.
    fn get_groups_and_results(table: &BaseHashTable) -> (Batch, Batch) {
        let num_groups = table.num_groups();
        let mut out_groups = Batch::new(table.layout.groups.types.clone(), num_groups).unwrap();
        let mut out_results = Batch::new(
            table
                .layout
                .aggregates
                .iter()
                .map(|agg| agg.function.state.return_type.clone()),
            num_groups,
        )
        .unwrap();

        let mut row_ptrs: Vec<_> = table.data.row_mut_ptr_iter().collect();
        assert_eq!(row_ptrs.len(), num_groups);

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

        out_groups.set_num_rows(num_groups).unwrap();
        out_results.set_num_rows(num_groups).unwrap();

        (out_groups, out_results)
    }

    #[test]
    fn single_insert_single_agg_no_group_vals() {
        // Hash table needs to handle case when no groups are provided. Happens
        // when producing a final rollup.

        // GROUP     NO USER PROVIDED GROUPS
        // GROUP     (hash): UInt64
        // AGG_INPUT (col0): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 0), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(0, DataType::int64())],
        )];

        let layout = AggregateLayout::try_new([DataType::uint64()], aggs).unwrap();

        let mut table = BaseHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = Batch::empty_with_num_rows(4);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);

        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

        let (out_groups, out_results) = get_groups_and_results(&table);

        // We should only have one "group", that being the hash value. We'll
        // have no user provided groups.

        let expected_groups = generate_array!([NO_GROUPS_HASH_VALUE]);
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);

        let expected_results = generate_batch!([10_i64]);
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn single_insert_single_agg_multiple_group_vals() {
        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout =
            AggregateLayout::try_new([DataType::utf8(), DataType::uint64()], aggs).unwrap();

        let mut table = BaseHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);

        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

        let (out_groups, out_results) = get_groups_and_results(&table);

        let expected_groups = Array::try_from_iter(["group_a", "group_b", "group_c"]).unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!([4_i64, 2, 4]);
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn num_group_values_greater_than_double_capacity() {
        // Tests that we properly resize the pointer directory when input has
        // more than twice as many unique group values than the current
        // capacity.
        //
        // Previously we just doubled, but that may not be enough. This
        // triggered an assertion failure since we were left with null pointers
        // for the group pointers.

        // GROUP     (col0): Int32
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout =
            AggregateLayout::try_new([DataType::int32(), DataType::uint64()], aggs).unwrap();

        let mut table = BaseHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let num_unique_groups = Directory::DEFAULT_CAPACITY * 2 + 1;

        let groups = generate_batch!(0..num_unique_groups as i32);
        // All inputs just have the same value.
        let inputs = generate_batch!(std::iter::repeat(4_i64).take(num_unique_groups));

        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

        let (out_groups, out_results) = get_groups_and_results(&table);

        let expected_groups = Array::try_from_iter(0..num_unique_groups as i32).unwrap();
        assert_arrays_eq(&expected_groups, &out_groups.arrays[0]);
        // Skip second groups array, contains hashes.

        let expected_results = generate_batch!(std::iter::repeat(4_i64).take(num_unique_groups));
        assert_batches_eq(&expected_results, &out_results);
    }

    #[test]
    fn multiple_inserts_single_agg_multiple_group_vals() {
        // GROUP     (col0): Utf8
        // GROUP     (hash): UInt64
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout =
            AggregateLayout::try_new([DataType::utf8(), DataType::uint64()], aggs).unwrap();

        let mut table = BaseHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

        let groups = generate_batch!(["group_c", "group_d", "group_a", "group_a"]);
        let inputs = generate_batch!([5_i64, 6, 7, 8]);
        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

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
            vec![expr::column((0, 1), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout =
            AggregateLayout::try_new([DataType::utf8(), DataType::uint64()], aggs).unwrap();

        let mut table = BaseHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

        let groups = generate_batch!(["group_c", "group_d"]);
        let inputs = generate_batch!([5_i64, 6]);
        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

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
            vec![expr::column((0, 1), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout =
            AggregateLayout::try_new([DataType::utf8(), DataType::uint64()], aggs).unwrap();

        let mut table = BaseHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

        let groups = generate_batch!(["group_d", "group_d", "group_e", "group_f"]);
        let inputs = generate_batch!([5_i64, 6, 7, 8]);
        hash_and_insert(&mut table, &mut state, &[0], &groups, &inputs);

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
            vec![expr::column((0, 1), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout =
            AggregateLayout::try_new([DataType::utf8(), DataType::uint64()], aggs).unwrap();

        let mut table = BaseHashTable::try_new(layout, 16).unwrap();
        let mut state = table.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        let hashes = Array::try_from_iter([1_u64, 1, 1, 1]).unwrap(); // All groups hashing to the same value
        table
            .insert_with_hashes(&mut state, &[0], &groups, &inputs, &hashes)
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
            vec![expr::column((0, 1), DataType::int64()).into()],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout =
            AggregateLayout::try_new([DataType::utf8(), DataType::uint64()], aggs).unwrap();

        let mut t1 = BaseHashTable::try_new(layout.clone(), 16).unwrap();
        let mut s1 = t1.init_insert_state();

        let groups = generate_batch!(["group_a", "group_b", "group_a", "group_c"]);
        let inputs = generate_batch!([1_i64, 2, 3, 4]);
        hash_and_insert(&mut t1, &mut s1, &[0], &groups, &inputs);

        let mut t2 = BaseHashTable::try_new(layout.clone(), 16).unwrap();
        let mut s2 = t2.init_insert_state();

        let groups = generate_batch!(["group_c", "group_d", "group_a", "group_a"]);
        let inputs = generate_batch!([5_i64, 6, 7, 8]);
        hash_and_insert(&mut t2, &mut s2, &[0], &groups, &inputs);

        t1.merge_many_into(&mut s2, [0], &mut [t2]).unwrap();
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
