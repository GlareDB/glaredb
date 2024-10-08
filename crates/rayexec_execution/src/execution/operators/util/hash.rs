// TODO: Move this?

/// Get the partition to use for a hash.
///
/// This should be used for hash repartitions, hash joins, hash aggregates, and
/// whatever else requires consistent hash to partition mappings.
pub const fn partition_for_hash(hash: u64, partitions: usize) -> usize {
    hash as usize % partitions
}
