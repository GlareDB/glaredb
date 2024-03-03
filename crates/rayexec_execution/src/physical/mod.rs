//! Planning and execution of physical plans.

pub mod chain;
pub mod planner;
pub mod plans;
pub mod scheduler;

use plans::{Sink, Source};
use std::sync::Arc;

use self::chain::OperatorChain;

#[derive(Debug)]
pub struct Pipeline {
    /// The operator chains that make up this pipeline.
    pub chains: Vec<Arc<OperatorChain>>,
}
