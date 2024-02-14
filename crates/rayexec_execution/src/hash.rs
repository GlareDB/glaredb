use crate::errors::{err, Result};
use ahash::RandomState;
use arrow_array::ArrayRef;

pub const RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

/// Hash every row in the provided arrays, writing the values to `hashes`.
pub fn build_hashes(arrays: &[&ArrayRef], hashes: &mut [u64]) -> Result<()> {
    unimplemented!()
}
