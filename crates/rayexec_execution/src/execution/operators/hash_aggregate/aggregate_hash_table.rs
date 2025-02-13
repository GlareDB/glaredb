use std::borrow::Borrow;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalU64, ScalarStorage};
use crate::arrays::array::raw::TypedRawBuffer;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_collection::{AggregateAppendState, AggregateCollection};
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::arrays::row::row_matcher::{MatchState, PredicateRowMatcher};
use crate::arrays::row::row_scan::RowScanState;
use crate::execution::operators::util::power_of_two::{
    compute_offset_from_hash,
    inc_and_wrap_offset,
    is_power_of_2,
};

// TODO: Rename?, used for inserting into the table but also for merging tables.
#[derive(Debug)]
pub struct AggregateHashTableInsertState {
    /// Reusable buffer containing pointers to the beginning of rows that we
    /// should update.
    row_ptrs: Vec<*mut u8>,
    /// State for appending to the aggregate collection.
    append_state: AggregateAppendState,
    /// State for matching on group values.
    match_state: MatchState,
}

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
    pub fn try_new(layout: AggregateLayout) -> Result<Self> {
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

        unimplemented!()
    }

    pub fn init_insert_state(&self) -> AggregateHashTableInsertState {
        unimplemented!()
        // InsertState{
        //     hashes: Vec::new(),
        //     row_ptrs: Vec::new(),
        //     append_state:
        // }
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
        debug_assert_eq!(groups.num_rows, inputs.num_rows);

        // Hash the groups.
        // TODO: Avoid allocating here.
        let mut hashes_arr = Array::new(&NopBufferManager, DataType::UInt64, groups.num_rows)?;
        let hashes = PhysicalU64::get_addressable_mut(&mut hashes_arr.data)?.slice;
        hash_many_arrays(&groups.arrays, 0..groups.num_rows, hashes)?;

        state.row_ptrs.clear();
        state
            .row_ptrs
            .extend(std::iter::repeat(std::ptr::null_mut()).take(groups.num_rows));

        self.find_or_create_groups(
            &mut state.append_state,
            &mut state.match_state,
            &groups.arrays,
            &hashes_arr,
            groups.num_rows,
            &mut state.row_ptrs,
        )?;

        debug_assert!(!state.row_ptrs.iter().any(|ptr| ptr.is_null()));

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
        match_state: &mut MatchState,
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

        if self.directory.needs_resize(num_rows) {
            self.directory.resize(self.directory.capacity() * 2)?;
        }

        // Precompute offsets into the table.
        let mut offsets = vec![num_rows; 0];
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
                let mut compare_ptrs: Vec<*const u8> = Vec::with_capacity(needs_compare.len());
                for &row_idx in &needs_compare {
                    // Offset updated during probing...
                    let offset = offsets[row_idx];
                    let row_ptr = entries[offset].row_ptr;
                    compare_ptrs.push(row_ptr as _);

                    // Set output pointer assuming it will match. If it doesn't,
                    // the following loop iteration(s) will overwrite it,
                    // eventually setting it the correct pointer.
                    out_ptrs[row_idx] = row_ptr;
                }

                let _ = self.matcher.find_matches(
                    match_state,
                    &self.layout,
                    &compare_ptrs,
                    &groups_and_hashes,
                    needs_compare.iter().copied(),
                )?;

                // For everything that didn't match, increment its offset so the
                // next iteration tries a new slot.
                for &not_match_row_idx in match_state.get_row_not_matches() {
                    let offset = &mut offsets[not_match_row_idx];
                    *offset = inc_and_wrap_offset(*offset, cap);
                }

                // Update selection to now only consider rows that didn't match.
                needs_insert.clear();
                needs_insert.extend(match_state.get_row_not_matches());
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
            self.find_or_create_groups(
                &mut state.append_state,
                &mut state.match_state,
                groups,
                hashes,
                num_rows,
                &mut state.row_ptrs,
            )?;

            // Combine states.
            unsafe {
                self.layout.combine_states(
                    merge_state.group_scan.scanned_row_pointers_mut(),
                    &mut state.row_ptrs,
                )?;
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

        Ok(MergeScanState {
            group_scan: RowScanState::new(),
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
    fn find_matches<A>(
        &self,
        state: &mut MatchState,
        layout: &AggregateLayout,
        lhs_rows: &[*const u8],
        rhs_columns: &[A],
        selection: impl IntoIterator<Item = usize>,
    ) -> Result<usize>
    where
        A: Borrow<Array>,
    {
        debug_assert_eq!(self.lhs_columns.len(), rhs_columns.len());
        self.matcher.find_matches(
            state,
            &layout.groups,
            lhs_rows,
            &self.lhs_columns,
            rhs_columns,
            selection,
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
    entries: TypedRawBuffer<Entry, NopBufferManager>,
}

impl Directory {
    const LOAD_FACTOR: f64 = 0.7;
    const DEFAULT_CAPACITY: usize = 128;

    fn try_new(capacity: usize) -> Result<Self> {
        let capacity = capacity.next_power_of_two();

        let mut entries = TypedRawBuffer::try_with_capacity(&NopBufferManager, capacity)?;
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

    fn resize(&mut self, new_capacity: usize) -> Result<()> {
        if !is_power_of_2(new_capacity) {
            return Err(RayexecError::new(
                "Hash table capacity needs to be a power of two",
            ));
        }
        if new_capacity < self.entries.capacity() {
            return Err(RayexecError::new("Cannot reduce capacity of hash table")
                .with_field("current", self.entries.capacity())
                .with_field("new", new_capacity));
        }

        let old_entries = std::mem::replace(
            &mut self.entries,
            TypedRawBuffer::try_with_capacity(&NopBufferManager, new_capacity)?,
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
        (num_inputs + self.num_occupied) as f64 / (self.capacity() * 2) as f64 >= Self::LOAD_FACTOR
    }
}
