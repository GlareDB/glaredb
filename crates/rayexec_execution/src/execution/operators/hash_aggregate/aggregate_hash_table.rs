use std::borrow::Borrow;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::raw::TypedRawBuffer;
use crate::arrays::array::Array;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::row::aggregate_collection::{AggregateAppendState, AggregateCollection};
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::arrays::row::row_matcher::{MatchState, PredicateRowMatcher};
use crate::execution::operators::util::power_of_two::{
    compute_offset_from_hash,
    inc_and_wrap_offset,
    is_power_of_2,
};

#[derive(Debug)]
pub struct InsertState {
    /// Reusable hashes buffer.
    hashes: Vec<u64>,
    /// Reusable buffer containing pointers to the beginning of rows that we
    /// should update.
    row_ptrs: Vec<*mut u8>,
    /// State for appending to the aggregate collection.
    append_state: AggregateAppendState,
    /// State for matching on group values.
    match_state: MatchState,
}

#[derive(Debug)]
pub struct MergeState {}

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
    layout: AggregateLayout,
    /// Hash table directory.
    directory: Directory,
    /// Byte offset to where a hash is stored in a row.
    hash_offset: usize,
    /// Hash table data storing the group and aggregate states.
    data: AggregateCollection,
    /// Group value matcher.
    matcher: GroupMatcher,
}

impl AggregateHashTable {
    pub fn init_insert_state(&self) -> InsertState {
        unimplemented!()
        // InsertState{
        //     hashes: Vec::new(),
        //     row_ptrs: Vec::new(),
        //     append_state:
        // }
    }

    pub fn insert(
        &mut self,
        state: &mut InsertState,
        groups: &[&Array],
        inputs: &[&Array],
        num_rows: usize,
    ) -> Result<()> {
        // Hash the groups.
        state.hashes.resize(num_rows, 0);
        hash_many_arrays(groups, 0..num_rows, &mut state.hashes)?;

        state.row_ptrs.clear();
        state
            .row_ptrs
            .extend(std::iter::repeat(std::ptr::null_mut()).take(num_rows));

        self.find_or_create_groups(
            &mut state.append_state,
            &mut state.match_state,
            groups,
            num_rows,
            &state.hashes,
            &mut state.row_ptrs,
        )?;

        debug_assert!(!state.row_ptrs.iter().any(|ptr| ptr.is_null()));

        // Update states.
        let agg_inputs: Vec<_> = self
            .layout
            .aggregates
            .iter()
            .flat_map(|agg| agg.columns.iter().map(|col| inputs[col.idx]))
            .collect();

        unsafe {
            self.layout
                .update_states(state.row_ptrs.as_mut_slice(), &agg_inputs, num_rows)?;
        }

        Ok(())
    }

    /// Find or create groups in the hash table.
    ///
    /// This will fill `out_ptrs` with the pointers to use for each row to
    /// update the aggregate state.
    fn find_or_create_groups(
        &mut self,
        append_state: &mut AggregateAppendState,
        match_state: &mut MatchState,
        groups: &[&Array],
        num_rows: usize,
        hashes: &[u64],
        out_ptrs: &mut [*mut u8],
    ) -> Result<()> {
        if num_rows == 0 {
            return Ok(());
        }

        debug_assert_eq!(num_rows, hashes.len());
        debug_assert_eq!(num_rows, out_ptrs.len());

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
                self.data
                    .append_groups(append_state, groups, new_groups.iter().copied())?;

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
                    groups,
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

    pub fn merge_from(&mut self, state: &mut InsertState, other: &mut Self) -> Result<()> {
        if self.directory.num_occupied == 0 {
            std::mem::swap(self, other);
            return Ok(());
        }
        if other.directory.num_occupied == 0 {
            return Ok(());
        }

        unimplemented!()
    }

    /// Get the hash for a row.
    ///
    /// Safety:
    ///
    /// Row pointer must be a pointer a row that follows this table's aggregate
    /// layout.
    unsafe fn row_hash(hash_offset: usize, row_ptr: *const u8) -> u64 {
        row_ptr.byte_add(hash_offset).cast::<u64>().read_unaligned()
    }
}

/// Wrapper around a predicate row matcher for matching on group values.
#[derive(Debug)]
struct GroupMatcher {
    /// Column indices we're matching on the left (should contain all indices).
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
struct Directory {
    /// Number of non-null pointers in entries.
    num_occupied: usize,
    /// Row pointers.
    entries: TypedRawBuffer<Entry, NopBufferManager>,
}

impl Directory {
    const LOAD_FACTOR: f64 = 0.7;

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

        unimplemented!()
    }

    /// Returns if the directory needs to be resized to accomadate new inputs.
    fn needs_resize(&self, num_inputs: usize) -> bool {
        (num_inputs + self.num_occupied) as f64 / (self.capacity() * 2) as f64 >= Self::LOAD_FACTOR
    }
}
