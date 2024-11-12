use std::collections::BTreeSet;
use std::sync::Arc;

use rayexec_bullet::array::Array;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::{RayexecError, Result};

use super::chunk::GroupChunk;
use super::compare::group_values_eq;
use super::drain::HashTableDrain;
use super::entry::EntryKey;
use super::Aggregate;

const LOAD_FACTOR: f64 = 0.7;

/// A linear probing hash table.
///
/// # Use of unsafe
///
/// Unsafe is used for the inner loops during probing to reduce bounds checking
/// when retrieving entry keys.
#[derive(Debug)]
pub struct HashTable {
    /// All chunks in the table.
    pub(crate) chunks: Vec<GroupChunk>,
    pub(crate) entries: Vec<EntryKey<GroupAddress>>,
    pub(crate) num_occupied: usize,
    pub(crate) insert_buffers: InsertBuffers,
    pub(crate) aggregates: Vec<Aggregate>,
}

/// Address to a single group in the hash table.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GroupAddress {
    pub chunk_idx: u16,
    pub row_idx: u16,
}

impl GroupAddress {
    const fn empty() -> Self {
        GroupAddress {
            chunk_idx: 0,
            row_idx: 0,
        }
    }
}

/// Reusable buffers during hash table inserts.
#[derive(Debug, Default)]
pub(crate) struct InsertBuffers {
    /// Computed offsets into entries.
    offsets: Vec<usize>,
    /// Selection vector containing indices for inputs rows that still need to
    /// be inserted into the table.
    needs_insert: SelectionVector,
    /// Selection vector pointing to new groups.
    new_group_rows: SelectionVector,
    /// Selection vector pointing to rows that need to be compared.
    needs_compare: SelectionVector,
    /// Rows that don't pass the equality check.
    not_eq_rows: BTreeSet<usize>,
    /// Group addresses for each row in the input.
    group_addresses: Vec<GroupAddress>,
    /// Chunks we'll be inserting into.
    // TODO: Try to remove this.
    chunk_indices: BTreeSet<u16>,
}

impl HashTable {
    /// Create a new hash table.
    ///
    /// `capacity` must be a power of 2.
    pub fn new(capacity: usize, aggregates: Vec<Aggregate>) -> Self {
        assert!(is_power_of_2(capacity));

        HashTable {
            chunks: Vec::new(),
            entries: vec![EntryKey::default(); capacity],
            num_occupied: 0,
            insert_buffers: InsertBuffers::default(),
            aggregates,
        }
    }

    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub fn insert(&mut self, groups: &[Array], hashes: &[u64], inputs: &[Array]) -> Result<()> {
        // Find and create groups as needed.
        self.find_or_create_groups(groups, hashes)?;

        // Now update aggregate states.
        //
        // We iterate the addresses to figure out which chunks actually need
        // upating.
        self.insert_buffers.chunk_indices.clear();
        self.insert_buffers.chunk_indices.extend(
            self.insert_buffers
                .group_addresses
                .iter()
                .map(|addr| addr.chunk_idx),
        );

        for &chunk_idx in &self.insert_buffers.chunk_indices {
            let chunk = &mut self.chunks[chunk_idx as usize];
            chunk.update_states(inputs, &self.insert_buffers.group_addresses)?;
        }

        Ok(())
    }

    pub fn merge_many(&mut self, others: &mut [HashTable]) -> Result<()> {
        let other_inputs: usize = others.iter().map(|table| table.num_occupied).sum();
        self.resize_if_needed(other_inputs)?;

        for other in others {
            self.merge(other)?;
        }

        Ok(())
    }

    pub fn merge(&mut self, other: &mut HashTable) -> Result<()> {
        self.resize_if_needed(other.num_occupied)?;

        for mut other_chunk in other.chunks.drain(..) {
            // Find or create groups in self from other.
            self.find_or_create_groups(&other_chunk.arrays, &other_chunk.hashes)?;

            // Now figure out which chunks we need to update in self. Find or
            // create groups would have already created new chunks with empty
            // states for us for groups we haven't seen in self.
            self.insert_buffers.chunk_indices.clear();
            self.insert_buffers.chunk_indices.extend(
                self.insert_buffers
                    .group_addresses
                    .iter()
                    .map(|addr| addr.chunk_idx),
            );

            for &chunk_idx in &self.insert_buffers.chunk_indices {
                let chunk = &mut self.chunks[chunk_idx as usize];
                chunk.combine_states(&mut other_chunk, &self.insert_buffers.group_addresses)?;
            }
        }

        Ok(())
    }

    pub fn into_drain(self) -> HashTableDrain {
        HashTableDrain {
            table: self,
            drain_idx: 0,
        }
    }

    fn find_or_create_groups(&mut self, groups: &[Array], hashes: &[u64]) -> Result<()> {
        let num_inputs = hashes.len();

        // Resize addresses, this will be where we store all the group
        // addresses that will be used during the state update.
        //
        // Existing values don't matter, they'll be overwritten as we update the
        // table.
        self.insert_buffers
            .group_addresses
            .resize(num_inputs, GroupAddress::default());

        // Note this check needs to be after the group address resize so that we
        // properly truncate it if we really have nothing.
        if num_inputs == 0 {
            return Ok(());
        }

        // Check to see if we should resize. Typically not all groups will
        // create a new entry, but it's possible so we need to account for that.
        self.resize_if_needed(num_inputs)?;

        // Precompute offsets into the table.
        self.insert_buffers.offsets.clear();
        self.insert_buffers.offsets.resize(num_inputs, 0);
        let cap = self.capacity() as u64;
        for (idx, &hash) in hashes.iter().enumerate() {
            self.insert_buffers.offsets[idx] = compute_offset_from_hash(hash, cap) as usize;
        }

        // Init selection to all rows in input.
        self.insert_buffers.needs_insert.clear();
        self.insert_buffers
            .needs_insert
            .append_locations(0..num_inputs);

        let mut remaining = num_inputs;

        // Number of new groups we've created.
        let mut new_groups = 0;

        let cap = self.capacity(); // So we don't need to do the cast in the inner loop.

        while remaining > 0 {
            // Pushed to as we occupy new entries.
            self.insert_buffers.new_group_rows.clear();
            // Pushed to as we find rows that need to be compared.
            self.insert_buffers.needs_compare.clear();
            // Pushed to during the equality check when hashes match.
            self.insert_buffers.not_eq_rows.clear();

            // Figure out where we're putting remaining rows.
            for idx in 0..remaining {
                let row_idx = self.insert_buffers.needs_insert.get(idx);
                let offset = &mut self.insert_buffers.offsets[row_idx];
                let row_hash = hashes[row_idx];

                // Probe
                for iter_count in 0..cap {
                    // SAFETY: Updates to `offset` wraps it around according to
                    // entries len.
                    let ent = unsafe { self.entries.get_unchecked_mut(*offset) };

                    if ent.is_empty() {
                        // Empty entry, claim it.
                        //
                        // Sets the prefix, but inserts an empty group address.
                        // The real group address will be figured out during
                        // state initalization.
                        *ent = EntryKey::new(hashes[row_idx], GroupAddress::empty());
                        self.insert_buffers.new_group_rows.push_location(row_idx);
                        new_groups += 1;
                        break;
                    }

                    // Entry not empty...

                    // Check if hash matches. If it does, we need to mark for
                    // comparison. If it doesn't we have linear probe.
                    if ent.hash == row_hash {
                        self.insert_buffers.needs_compare.push_location(row_idx);
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

            // If we've inserted new group hashes, go ahead and create the actual
            // groups.
            if !self.insert_buffers.new_group_rows.is_empty() {
                // TODO: Try not to clone?
                let selection = Arc::new(self.insert_buffers.new_group_rows.clone());

                let group_vals = groups.iter().map(|a| {
                    let mut arr = a.clone();
                    arr.select_mut(selection.clone());
                    arr
                });

                let phys_types = groups.iter().map(|a| a.physical_type());

                let num_new_groups = self.insert_buffers.new_group_rows.len();

                // Get the chunk to insert into and the relative offset within
                // that chunk. The offset is used to ensure we update the hash
                // table entry with the true row idx within the chunk in the
                // case of chunk reuse.
                let (chunk_idx, chunk_offset) = match self.chunks.last_mut() {
                    Some(chunk) if chunk.can_append(num_new_groups, phys_types) => {
                        let chunk_offset = chunk.num_groups;

                        // Append to previous chunk.
                        let hashes = self
                            .insert_buffers
                            .new_group_rows
                            .iter_locations()
                            .map(|loc| hashes[loc]);

                        chunk.append_group_values(group_vals, hashes)?;

                        let chunk_idx = self.chunks.len() - 1;
                        (chunk_idx, chunk_offset)
                    }
                    _ => {
                        // Either we have no chunk, or we do but it's already at
                        // a good capacity. Create a new one.
                        let chunk_idx = self.chunks.len();
                        let mut states = self
                            .aggregates
                            .iter()
                            .map(|agg| agg.new_states())
                            .collect::<Result<Vec<_>>>()?;

                        // Initialize the states.
                        for state in &mut states {
                            state.states.new_groups(num_new_groups);
                        }

                        let chunk = GroupChunk {
                            chunk_idx: chunk_idx as u16,
                            num_groups: num_new_groups,
                            hashes: self
                                .insert_buffers
                                .new_group_rows
                                .iter_locations()
                                .map(|loc| hashes[loc])
                                .collect(),
                            arrays: group_vals.collect(),
                            aggregate_states: states,
                        };
                        self.chunks.push(chunk);

                        (chunk_idx, 0)
                    }
                };

                // Update hash table entries to point to the new chunk.
                //
                // Accounts for the selection we did when putting the arrays
                // into the chunk.
                for (updated_idx, row_idx) in self
                    .insert_buffers
                    .new_group_rows
                    .iter_locations()
                    .enumerate()
                {
                    let offset = self.insert_buffers.offsets[row_idx];
                    let ent = &mut self.entries[offset];

                    let addr = GroupAddress {
                        chunk_idx: chunk_idx as u16,
                        row_idx: (updated_idx + chunk_offset) as u16,
                    };

                    *ent = EntryKey::new(hashes[row_idx], addr);

                    // Update output addresses too.
                    self.insert_buffers.group_addresses[row_idx] = addr;
                }
            }

            // We have rows to compare.
            if !self.insert_buffers.needs_compare.is_empty() {
                // Update addresses slice with the groups we'll be comparing
                // against.
                for row_idx in self.insert_buffers.needs_compare.iter_locations() {
                    let offset = self.insert_buffers.offsets[row_idx];
                    let ent = &self.entries[offset];
                    // Sets address for this row to existing group. If the rows
                    // are actually equal, then this remains as is. Otherwise
                    // the next iteration(s) of the loop will update this to
                    // keep trying to compare.
                    self.insert_buffers.group_addresses[row_idx] = ent.key;
                }

                // Compare our input groups to the existing groups.

                // Figure out which chunks we're comparing against.
                self.insert_buffers.chunk_indices.clear();
                self.insert_buffers.chunk_indices.extend(
                    self.insert_buffers
                        .group_addresses
                        .iter()
                        .map(|addr| addr.chunk_idx),
                );

                // Do the actual compare.
                group_values_eq(
                    groups,
                    &self.insert_buffers.needs_compare,
                    &self.chunks,
                    &self.insert_buffers.group_addresses,
                    &self.insert_buffers.chunk_indices,
                    &mut self.insert_buffers.not_eq_rows,
                )?;
            }

            // Now for every row that failed the equality check, increment its
            // offset to try the next entry in the table.
            for &row_idx in &self.insert_buffers.not_eq_rows {
                let offset = &mut self.insert_buffers.offsets[row_idx];
                *offset = inc_and_wrap_offset(*offset, cap);
            }

            // Now try next iteration just with rows that failed the equality
            // check.
            self.insert_buffers.needs_insert.clear();
            self.insert_buffers
                .needs_insert
                .extend(self.insert_buffers.not_eq_rows.iter().copied());

            remaining = self.insert_buffers.needs_insert.len();
        }

        self.num_occupied += new_groups;

        Ok(())
    }

    fn resize(&mut self, new_capacity: usize) -> Result<()> {
        assert!(is_power_of_2(new_capacity));

        if new_capacity < self.entries.len() {
            return Err(RayexecError::new("Cannot reduce capacity"));
        }

        let mut new_entries = vec![EntryKey::default(); new_capacity];

        for ent in self.entries.drain(..) {
            let mut offset = ent.hash as usize % new_capacity;

            // Keep looping until we find an empty entry.
            for _iter_count in 0..new_capacity {
                // SAFETY: `offset` is wrapped according to new capacity which
                // corresponds to new entries length.
                let ent = unsafe { new_entries.get_unchecked(offset) };
                if ent.is_empty() {
                    break;
                }

                offset = inc_and_wrap_offset(offset, new_capacity)
            }

            new_entries[offset] = ent;
        }

        self.entries = new_entries;

        Ok(())
    }

    /// Resize the hash table if needed, keeping the load factor less than
    /// LOAD_FACTOR.
    fn resize_if_needed(&mut self, num_inputs: usize) -> Result<()> {
        let possible_occupied = num_inputs + self.num_occupied;

        // Calculate the minimum required capacity.
        let mut new_capacity = self.capacity();
        while (possible_occupied as f64) / (new_capacity as f64) >= LOAD_FACTOR {
            new_capacity *= 2;
        }

        // Now resize if needed.
        if new_capacity != self.capacity() {
            self.resize(new_capacity)?;
        }

        Ok(())
    }
}

/// Increment offset, wrapping if necessary.
///
/// Requires that `cap` be a power of 2.
const fn inc_and_wrap_offset(offset: usize, cap: usize) -> usize {
    (offset + 1) & (cap - 1)
}

/// Compute the initial offset using a hash.
///
/// Requires that `cap` be a power of 2.
const fn compute_offset_from_hash(hash: u64, cap: u64) -> u64 {
    hash & (cap - 1)
}

const fn is_power_of_2(v: usize) -> bool {
    (v & (v - 1)) == 0
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::bitmap::Bitmap;
    use rayexec_bullet::datatype::DataType;

    use super::*;
    use crate::functions::aggregate::sum::Sum;
    use crate::functions::aggregate::AggregateFunction;

    fn make_hash_table() -> HashTable {
        let aggregate = Aggregate {
            function: Sum.plan_from_datatypes(&[DataType::Int64]).unwrap(),
            col_selection: Bitmap::from_iter([true]),
            is_distinct: false,
        };

        HashTable::new(16, vec![aggregate])
    }

    #[test]
    fn insert_simple() {
        let groups = [Array::from_iter(["g1", "g2", "g1"])];
        let inputs = [Array::from_iter::<[i64; 3]>([1, 2, 3])];

        let hashes = [4, 5, 4]; // Hashes for group values.

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(2, table.num_occupied);
    }

    #[test]
    fn insert_chunk_append() {
        // Assumes knowledge of internals.

        let groups1 = [Array::from_iter(["g1", "g2", "g1"])];
        let inputs1 = [Array::from_iter::<[i64; 3]>([1, 2, 3])];
        let hashes1 = [4, 5, 4];

        let groups2 = [Array::from_iter(["g1", "g2", "g3"])];
        let inputs2 = [Array::from_iter::<[i64; 3]>([1, 2, 3])];
        let hashes2 = [4, 5, 6];

        let mut table = make_hash_table();
        table.insert(&groups1, &hashes1, &inputs1).unwrap();
        table.insert(&groups2, &hashes2, &inputs2).unwrap();

        assert_eq!(3, table.num_occupied);
        assert_eq!(1, table.chunks.len());
    }

    #[test]
    fn insert_hash_collision() {
        let groups = [Array::from_iter(["g1", "g2", "g1"])];
        let inputs = [Array::from_iter::<[i64; 3]>([1, 2, 3])];

        let hashes = [4, 4, 4];

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(2, table.num_occupied);
    }

    #[test]
    fn insert_require_resize() {
        // 17 unique groups (> initial 16 capacity)

        let groups = [Array::from_iter(0..17)];
        let inputs = [Array::from_iter(0 as i64..17 as i64)];

        let hashes = vec![44; 17]; // All hashes collide.

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(17, table.num_occupied);
    }

    #[test]
    fn insert_require_resize_more_than_double() {
        // 33 unique groups, more than twice initial capacity. Caught bug where
        // resize by doubling didn't increase capacity enough.

        let groups = [Array::from_iter(0..33)];
        let inputs = [Array::from_iter(0 as i64..33 as i64)];

        let hashes = vec![44; 33]; // All hashes collide.

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(33, table.num_occupied);
    }

    #[test]
    fn merge_simple() {
        let groups1 = [Array::from_iter(["g1", "g2", "g1"])];
        let inputs1 = [Array::from_iter::<[i64; 3]>([1, 2, 3])];

        let hashes = vec![4, 5, 4];
        let mut t1 = make_hash_table();
        t1.insert(&groups1, &hashes, &inputs1).unwrap();

        let groups2 = [Array::from_iter(["g3", "g2", "g1"])];
        let inputs2 = [Array::from_iter::<[i64; 3]>([1, 2, 3])];

        let hashes = vec![6, 5, 4];

        let mut t2 = make_hash_table();
        t2.insert(&groups2, &hashes, &inputs2).unwrap();

        t1.merge_many(&mut [t2]).unwrap();

        assert_eq!(3, t1.num_occupied);
    }
}
