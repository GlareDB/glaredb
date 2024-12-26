use std::sync::atomic::AtomicU64;

use rayexec_error::Result;

use super::block::HashedBlock;
use crate::arrays::buffer_manager::BufferManager;

pub const EMPTY_BLOCK_ID: u32 = 0;

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct HashTableEntry {
    /// ID of block containing this entry's row.
    block_id: u32,
    /// Index of the row within the block.
    row_idx: u16,
    /// 16-bit prefix of the hash this entry represents.
    prefix: u16,
}

impl HashTableEntry {
    /// Assertions about size/alignment of the entry.
    ///
    /// We need the entry to be trasnmutable to u64/AtomicU64 to allow for parallel
    /// builds of the hash table.
    const _ENTRY_ASSERTIONS: () = {
        assert!(std::mem::align_of::<u64>() == std::mem::align_of::<HashTableEntry>());
        assert!(std::mem::size_of::<u64>() == std::mem::size_of::<HashTableEntry>());

        // u64 can be of a different alignment on some targets.
        // TODO: Which targets?
        assert!(std::mem::align_of::<AtomicU64>() == std::mem::align_of::<u64>());

        // An empty entry must equal 0_u64 for easy comparison.
        assert!(HashTableEntry::empty().as_u64() == 0);
    };

    pub const fn empty() -> Self {
        HashTableEntry {
            block_id: 0,
            row_idx: 0,
            prefix: 0,
        }
    }

    pub const fn as_u64(&self) -> u64 {
        unsafe { std::mem::transmute(*self) }
    }

    const fn transmuted_u64_is_empty(v: u64) -> bool {
        v == 0
    }

    /// Get the hash prefix from a hash entry that's been transmuted to a u64.
    const fn hash_prefix_from_transmuted_u64(v: u64) -> u16 {
        const MASK: u64 = 0xFFFF_0000_0000_0000;
        ((v & MASK) >> 48) as u16
    }

    const fn row_idx_from_transmuted_u64(v: u64) -> u16 {
        const MASK: u64 = 0x0000_FFFF_0000_0000;
        ((v & MASK) >> 32) as u16
    }

    const fn block_idx_from_transmuted_u64(v: u64) -> u32 {
        const MASK: u64 = 0x0000_0000_FFFF_FFFF;
        (v & MASK) as u32
    }
}

/// Resusable buffers used during block inserts.
#[derive(Debug)]
pub struct InsertBuffers {
    /// Offsets computed from incoming hashes.
    offsets: Vec<usize>,
}

#[derive(Debug)]
pub struct JoinHashTable {
    slots: Vec<HashTableEntry>,
}

impl JoinHashTable {
    /// Create a new hash table with at least the given capacity.
    ///
    /// If capacity is not a power of 2, it will be rounded up to the next power
    /// of 2.
    pub fn new(capacity: usize) -> Result<Self> {
        let capacity = capacity.next_power_of_two();

        Ok(JoinHashTable {
            slots: vec![HashTableEntry::empty(); capacity],
        })
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub fn insert_hashed_block<B: BufferManager>(
        &self,
        insert_bufs: &mut InsertBuffers,
        block: &HashedBlock<B>,
    ) -> Result<()> {
        let row_count = block.block.row_count();

        let offsets = &mut insert_bufs.offsets;
        offsets.clear();
        offsets.resize(row_count, 0);

        // Compute offsets from hashes.
        let cap = self.capacity() as u64;
        for (idx, &hash) in block.hashes.iter().take(row_count).enumerate() {
            offsets[idx] = compute_offset_from_hash(hash, cap) as usize;
        }

        let slots = self.slots.as_slice();
        // SAFETY: Const assert that alignemnt/size of entry is equal to u64,
        // and that alignment of u64 is equal to AtomicU64.
        let slots = unsafe { std::mem::transmute::<&[HashTableEntry], &[AtomicU64]>(slots) };

        // Insert each row in the block.
        for row_idx in 0..row_count {}

        unimplemented!()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_from_transmuted_entry() {
        let ent = HashTableEntry {
            block_id: 4,
            row_idx: 5,
            prefix: 0x4ea3,
        }
        .as_u64();

        assert_eq!(0x4ea3, HashTableEntry::hash_prefix_from_transmuted_u64(ent));
        assert_eq!(5, HashTableEntry::row_idx_from_transmuted_u64(ent));
        assert_eq!(4, HashTableEntry::block_idx_from_transmuted_u64(ent));
    }
}
