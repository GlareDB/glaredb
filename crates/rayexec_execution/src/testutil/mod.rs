//! Test utilities.
//!
//! Note modules are selectively given the `#[cfg(test)]` attribute depending on
//! if they're useful outside of this crate.
//!
//! Should not be used outside of tests.
//!
//! Some helpers are useful in a variety of places. The module structure
//! attempts to group these in some manner while also keeping the file structure
//! flat. This should help with discoverability and reduce duplicated helpers.

#![allow(unused)]

/// Array helpers.
pub mod arrays {
    pub use super::array_assert::*;
    pub use super::generate_batch::*;
    pub use super::row_blocks::*;
}

mod array_assert;
mod generate_batch;
mod row_blocks;

/// Operator helpers.
pub mod operator {
    pub use super::operator_wrapper::*;
}

mod operator_wrapper;

/// Expression helpers.
pub mod exprs {
    pub use super::plan_exprs::*;
}

mod plan_exprs;

pub mod database_context;
