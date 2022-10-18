//! Physical execution plans.
pub mod delta_inserts;
pub mod local_partition;
pub mod select_unordered;
pub mod trace;

pub use delta_inserts::*;
pub use local_partition::*;
pub use select_unordered::*;
pub use trace::*;
