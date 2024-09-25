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
