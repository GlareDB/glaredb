//! Join implementations and utilities.

pub mod hash_join;
pub mod nested_loop_join;

mod cross_product;
mod hash_table_entry;
mod join_hash_table;
mod outer_join_tracker;

use crate::logical::logical_join::JoinType;

/// If the result of a join should be empty if the build input is empty.
///
/// Assumes "left" is the build side.
pub const fn empty_output_on_empty_build(typ: JoinType) -> bool {
    match typ {
        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => true,
        JoinType::Full | JoinType::Right | JoinType::LeftMark { .. } => false,
    }
}

/// If this join produces all rows from the build side.
///
/// Assumes "left" is the build side.
pub const fn produce_all_build_side_rows(typ: JoinType) -> bool {
    match typ {
        JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::LeftSemi => true,
        JoinType::Inner | JoinType::Right | JoinType::LeftMark { .. } => false,
    }
}
