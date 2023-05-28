//! Push based exection with adapters for datafusion.
pub mod errors;
pub mod operator;
pub mod plan;
pub mod scheduler;

mod adapter;
mod partition;
mod repartition;
