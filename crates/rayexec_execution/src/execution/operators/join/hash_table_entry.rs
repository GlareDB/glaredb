use crate::arrays::collection::row::RowAddress;

/// An entry in the join hash table pointing to a row in a chunk.
///
/// This is expected to be interchangeable with the hash value for a row (so
/// must be 64 bits)
///
/// Entries with all fields set to zero are considered dangling, meaning it does
/// not point to a row.
// TODO: This will leave some performance on the table compared to raw pointers,
// but don't want to get into the mess of pointers in rust yet (whatever the
// "provenance" stuff is).
//
// However for something like an "unchained" hash table, this might be
// sufficient: <https://db.in.tum.de/~birler/papers/hashtable.pdf>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, align(8))]
pub struct HashTableEntry {
    /// Index of the chunk containing this row.
    pub chunk_idx: u32,
    /// The row index within the chunks.
    ///
    /// This effectively limits the max batch size to 65535.
    pub row_idx: u16,
    /// Prefix of the hash.
    pub hash_prefix: u16,
}

impl HashTableEntry {
    const _SIZE_ASSERTION: () = assert!(std::mem::size_of::<Self>() == std::mem::size_of::<u64>());
    const _ALIGN_ASSERTION: () =
        assert!(std::mem::align_of::<Self>() == std::mem::align_of::<u64>());

    pub const DANGLING: Self = HashTableEntry {
        chunk_idx: 0,
        row_idx: 0,
        hash_prefix: 0,
    };

    pub const DANGLING_U64: u64 = Self::DANGLING.as_u64();

    pub const fn new(addr: RowAddress, hash: u64) -> Self {
        HashTableEntry {
            chunk_idx: addr.chunk_idx,
            row_idx: addr.row_idx,
            hash_prefix: hash_prefix(hash),
        }
    }

    pub const fn row_address(&self) -> RowAddress {
        RowAddress {
            chunk_idx: self.chunk_idx,
            row_idx: self.row_idx,
        }
    }

    pub const fn is_dangling(self) -> bool {
        self.as_u64() == Self::DANGLING_U64
    }

    /// Converts self to a u64.
    pub const fn as_u64(self) -> u64 {
        unsafe { std::mem::transmute::<Self, u64>(self) }
    }

    /// Converts a u64 to a hash table entry.
    pub const fn from_u64(v: u64) -> Self {
        unsafe { std::mem::transmute::<u64, Self>(v) }
    }

    pub const fn u64_slice_to_entries(vals: &[u64]) -> &[HashTableEntry] {
        unsafe { std::mem::transmute::<&[u64], &[Self]>(vals) }
    }
}

/// Generate a 16 bit prefix for the hash.
pub const fn hash_prefix(hash: u64) -> u16 {
    (hash >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_round_trip() {
        let entries = [
            HashTableEntry {
                chunk_idx: 0,
                row_idx: 0,
                hash_prefix: 0xffff,
            },
            HashTableEntry {
                chunk_idx: 1,
                row_idx: 2,
                hash_prefix: 0xabcd,
            },
        ];

        for ent in entries {
            let u = ent.as_u64();
            let got = HashTableEntry::from_u64(u);
            assert_eq!(ent, got);
        }
    }

    #[test]
    fn u64_slice_to_ents() {
        let entries = [
            HashTableEntry {
                chunk_idx: 0,
                row_idx: 0,
                hash_prefix: 0xffff,
            },
            HashTableEntry {
                chunk_idx: 1,
                row_idx: 2,
                hash_prefix: 0xabcd,
            },
        ];

        let vals: Vec<_> = entries.iter().map(|ent| ent.as_u64()).collect();
        let got = HashTableEntry::u64_slice_to_entries(&vals);

        assert_eq!(entries, got);
    }
}
