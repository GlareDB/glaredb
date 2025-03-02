//! Intermediate pipeline planning.
//!
//! This module holds both the types for intermediate pipelines, and the planner
//! that produces the pipelines from logical operators.
//!
//! Intermediate pipelines represent which operators will be taking part in
//! query execution, but without operator-specific states. This easily lets us
//! serialize plans for remote execution, as well as delaying assigning plan
//! parallelism until we actually want to execute the pipelines.

pub mod pipeline;
// pub mod planner;
