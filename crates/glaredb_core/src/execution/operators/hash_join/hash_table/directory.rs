use std::sync::atomic::AtomicPtr;

use glaredb_error::Result;

use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::buffer::db_vec::DbVec;

/// (Chained) hash table directory.
///
/// Each entry is a row pointer pointing to the front of a chain. Each row will
/// point to the next entry in the chain through a serialized pointer. The end
/// of a chain is denoted by a null pointer.
#[derive(Debug)]
pub struct Directory {
    entries: DbVec<*mut u8>,
}

// `*mut u8` pointing to heap blocks.
unsafe impl Sync for Directory {}
unsafe impl Send for Directory {}

const _: () = assert!(
    Directory::MIN_SIZE.is_power_of_two(),
    "Min directory size needs to be a power of two"
);

impl Directory {
    const MIN_SIZE: usize = 256;
    const LOAD_FACTOR: f64 = 0.7;

    /// Mask to use when determining the position for an entry in the hash
    /// table.
    pub const fn capacity_mask(&self) -> u64 {
        self.entries.len() as u64 - 1
    }

    /// Create a new directory for the given number of rows.
    ///
    /// This will ensure the hash table is an appropriate size and that the size
    /// is a power of two for efficient computing of offsets.
    pub fn new_for_num_rows(num_rows: usize) -> Result<Self> {
        let desired = (num_rows as f64 / Self::LOAD_FACTOR) as usize;
        let actual = usize::max(desired.next_power_of_two(), Self::MIN_SIZE);

        let entries = DbVec::with_value(&DefaultBufferManager, actual, std::ptr::null_mut())?;

        Ok(Directory { entries })
    }

    pub fn get_entry(&self, idx: usize) -> *const u8 {
        debug_assert!(idx < self.entries.len());
        let ptr = unsafe { self.entries.as_ptr().add(idx) };
        unsafe { *ptr }
    }

    pub fn get_entry_atomic(&self, idx: usize) -> &AtomicPtr<u8> {
        debug_assert!(idx < self.entries.len());
        // TODO: Need to figure out the mutability for the directory.
        let ptr = self.entries.as_ptr().cast_mut();
        let ptr = unsafe { ptr.add(idx) };
        unsafe { AtomicPtr::from_ptr(ptr) }
    }
}
