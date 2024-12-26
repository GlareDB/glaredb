//! Scalar executors for generic vectorized execution over different types of
//! arrays.
//!
//! Structs may be extended to include a buffer in the future to avoid
//! operations having to allows strings or vecs when operating on string and
//! binary arrays.
//!
//! Explicit generic typing is used for unary, binary, and ternary operations as
//! those are likely to be the most common, so have these operations be
//! monomorphized is probably a good thing.

mod unary;
pub use unary::*;

mod binary;
pub use binary::*;

mod ternary;
pub use ternary::*;

mod uniform;
pub use uniform::*;

mod select;
pub use select::*;

mod hash;
pub use hash::*;

mod list;
pub use list::*;

mod fill;
pub use fill::*;
use rayexec_error::{RayexecError, Result};

use super::builder::ArrayDataBuffer;
use crate::arrays::array::Array;
use crate::arrays::bitmap::Bitmap;

#[inline]
pub fn check_validity(idx: usize, validity: Option<&Bitmap>) -> bool {
    match validity {
        Some(v) => v.value(idx),
        None => true,
    }
}

pub fn can_skip_validity_check<'a, I>(validities: I) -> bool
where
    I: IntoIterator<Item = Option<&'a Bitmap>>,
    I::IntoIter: ExactSizeIterator,
{
    for validity in validities.into_iter().flatten() {
        if !validity.is_all_true() {
            return false;
        }
    }

    true
}

/// Validates that the length of a buffer that we're using for building a new
/// array matches the logical length of some other array.
///
/// Returns the logical length.
pub(crate) fn validate_logical_len<B>(buffer: &B, array: &Array) -> Result<usize>
where
    B: ArrayDataBuffer,
{
    let len = buffer.len();
    if buffer.len() != array.logical_len() {
        return Err(RayexecError::new(format!(
            "Invalid lengths, buffer len: {}, array len: {}",
            buffer.len(),
            array.logical_len(),
        )));
    }
    Ok(len)
}
