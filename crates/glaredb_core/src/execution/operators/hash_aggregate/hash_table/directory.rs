use std::ptr::NonNull;

use glaredb_error::{DbError, Result};

use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::buffer::db_vec::DbVec;

#[derive(Debug, Clone, Copy)]
pub struct Entry {
    /// The hash value for this entry.
    pub hash: u64,
    /// The pointer to the start of the row.
    ///
    /// None if this slot isn't occupied.
    pub ptr: Option<NonNull<u8>>,
}

impl Entry {
    const ZERO: Self = Entry { hash: 0, ptr: None };
}

/// Hash table directory containing pointers to rows.
///
/// The number of entries in the directory indicates the number of groups.
#[derive(Debug)]
pub struct Directory {
    /// Number of non-null pointers in entries.
    pub num_occupied: usize,
    /// Entries in the table.
    pub entries: DbVec<Entry>,
}

// Pointers point to heap blocks.
unsafe impl Send for Directory {}
unsafe impl Sync for Directory {}

const _: () = {
    assert!(
        Directory::DEFAULT_CAPACITY.is_power_of_two(),
        "must be power of two"
    );
};

impl Directory {
    const LOAD_NUM: usize = 7;
    const LOAD_DEN: usize = 10;

    pub const DEFAULT_CAPACITY: usize = 512;

    pub fn try_with_capacity(capacity: usize) -> Result<Self> {
        let capacity = capacity.next_power_of_two();

        let entries = DbVec::with_value(&DefaultBufferManager, capacity, Entry::ZERO)?;

        Ok(Directory {
            num_occupied: 0,
            entries,
        })
    }

    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// Resizes the directory to at least `new_capacity`.
    ///
    /// This will ensure the new capacity of the directory is a power of two.
    pub fn resize(&mut self, mut new_capacity: usize) -> Result<()> {
        if !is_power_of_two(new_capacity) {
            new_capacity = new_capacity
                .checked_next_power_of_two()
                .ok_or_else(|| DbError::new("Requested capacity for directory to high"))?;
        }
        if new_capacity < self.entries.len() {
            return Err(DbError::new("Cannot reduce capacity of hash table")
                .with_field("current", self.entries.len())
                .with_field("new", new_capacity));
        }

        let old_entries = std::mem::replace(
            &mut self.entries,
            DbVec::with_value(&DefaultBufferManager, new_capacity, Entry::ZERO)?,
        );
        let entries = self.entries.as_slice_mut();

        for ent in old_entries.as_slice() {
            if ent.ptr.is_none() {
                continue;
            }

            let mut offset = compute_offset_from_hash(ent.hash, new_capacity as u64) as usize;
            // Continue to try to insert until we find an empty slot.
            loop {
                if entries[offset].ptr.is_none() {
                    // Empty slot, insert entry.
                    entries[offset] = *ent;
                    break;
                }

                // Keep probing.
                offset = inc_and_wrap_offset(offset, new_capacity);
            }
        }

        debug_assert_eq!(
            self.num_occupied,
            self.entries
                .as_slice()
                .iter()
                .filter(|ent| ent.ptr.is_some())
                .count()
        );

        Ok(())
    }

    pub fn needs_resize(&self, num_inputs: usize) -> bool {
        // (num_occupied + num_inputs) / capacity > 7/10
        (self.num_occupied + num_inputs) * Self::LOAD_DEN > self.capacity() * Self::LOAD_NUM
    }
}

/// Increment offset by one, wrapping around if necessary.
///
/// Requires that `cap` be a power of 2.
pub const fn inc_and_wrap_offset(offset: usize, cap: usize) -> usize {
    (offset + 1) & (cap - 1)
}

/// Compute the initial offset using a hash.
///
/// Requires that `cap` be a power of 2.
pub const fn compute_offset_from_hash(hash: u64, cap: u64) -> u64 {
    hash & (cap - 1)
}

/// Returns if `v` is a power of two.
pub const fn is_power_of_two(v: usize) -> bool {
    (v & (v - 1)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_resize() {
        let mut dir = Directory::try_with_capacity(512).unwrap();
        dir.num_occupied = 253;

        let needs_resize = dir.needs_resize(267);
        assert!(needs_resize);
    }
}
