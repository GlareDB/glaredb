use std::ptr::NonNull;

use glaredb_error::{DbError, Result};

use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::buffer::db_vec::DbVec;

/// Hash table directory containing pointers to rows.
///
/// The number of entries in the directory indicates the number of groups.
#[derive(Debug)]
pub struct Directory {
    /// Number of non-null pointers in entries.
    pub num_occupied: usize,
    /// Row pointers.
    ///
    /// None if this slot isn't occupied.
    pub ptrs: DbVec<Option<NonNull<u8>>>,
    /// Hashes for each slot in the directory.
    pub hashes: DbVec<u64>,
}

// Pointers point to heap blocks.
unsafe impl Send for Directory {}
unsafe impl Sync for Directory {}

impl Directory {
    const LOAD_NUM: usize = 7;
    const LOAD_DEN: usize = 10;

    pub const DEFAULT_CAPACITY: usize = 128;

    pub fn try_with_capacity(capacity: usize) -> Result<Self> {
        let capacity = capacity.next_power_of_two();

        let ptrs = DbVec::with_value(&DefaultBufferManager, capacity, None)?;
        let hashes = DbVec::with_value(&DefaultBufferManager, capacity, 0)?;

        Ok(Directory {
            num_occupied: 0,
            ptrs,
            hashes,
        })
    }

    pub fn capacity(&self) -> usize {
        self.ptrs.len()
    }

    /// Resizes the directory to at least `new_capacity`.
    ///
    /// This will ensure the new capacity of the directory is a power of two.
    pub fn resize(&mut self, mut new_capacity: usize) -> Result<()> {
        if !is_power_of_2(new_capacity) {
            new_capacity = new_capacity.next_power_of_two();
        }
        if new_capacity < self.ptrs.len() {
            return Err(DbError::new("Cannot reduce capacity of hash table")
                .with_field("current", self.ptrs.len())
                .with_field("new", new_capacity));
        }

        let old_ptrs = std::mem::replace(
            &mut self.ptrs,
            DbVec::with_value(&DefaultBufferManager, new_capacity, None)?,
        );
        let old_hashes = std::mem::replace(
            &mut self.hashes,
            DbVec::with_value(&DefaultBufferManager, new_capacity, 0)?,
        );

        let hashes = self.hashes.as_slice_mut();
        let ptrs = self.ptrs.as_slice_mut();

        for (&hash, &ptr) in old_hashes.as_slice().iter().zip(old_ptrs.as_slice()) {
            if ptr.is_none() {
                continue;
            }

            let mut offset = compute_offset_from_hash(hash, new_capacity as u64) as usize;
            // Continue to try to insert until we find an empty slot.
            loop {
                if ptrs[offset].is_none() {
                    // Empty slot, insert entry.
                    ptrs[offset] = ptr;
                    hashes[offset] = hash;
                    break;
                }

                // Keep probing.
                offset = inc_and_wrap_offset(offset, new_capacity);
            }
        }

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
pub const fn is_power_of_2(v: usize) -> bool {
    (v & (v - 1)) == 0
}
