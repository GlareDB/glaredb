use ahash::RandomState;
use arrow_array::ArrayRef;
use rayexec_error::Result;

/// State used for all hashing operations during physical execution.
pub const HASH_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

/// Hash every row in the provided arrays, writing the values to `hashes`.
pub fn build_hashes(_arrays: &[&ArrayRef], _hashes: &mut [u64]) -> Result<()> {
    unimplemented!()
}
