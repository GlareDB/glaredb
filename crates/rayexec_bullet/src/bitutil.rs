/// Get the number of bytes that can fit `num_bits`.
pub const fn byte_ceil(num_bits: usize) -> usize {
    ceil(num_bits, 8)
}

/// Get the ceil of n/divisor.
pub const fn ceil(n: usize, divisor: usize) -> usize {
    n / divisor + (0 != n % divisor) as usize
}
