//! Push based exection with adapters for datafusion.
pub mod adapter;
pub mod errors;
pub mod pipeline;
pub mod plan;
pub mod scheduler;

mod partition;
mod repartition;
