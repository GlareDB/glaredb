//! Join implementations and utilities.

use crate::logical::logical_join::JoinType;
pub mod nested_loop_join;

mod cross_product;
mod outer_join_tracker;

/// If the result of a join should be empty if the build input is empty.
///
/// Assumes "left" is the build side.
pub const fn empty_output_on_empty_build(typ: JoinType) -> bool {
    match typ {
        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => true,
        JoinType::Full | JoinType::Right | JoinType::LeftMark { .. } => false,
    }
}
