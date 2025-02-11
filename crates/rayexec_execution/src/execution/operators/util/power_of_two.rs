//! Power of two helpers for the join and aggregate hash tables.

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
