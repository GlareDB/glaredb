//! Planning and execution of physical plans.

pub mod datasource;
pub mod planner;
pub mod plans;
pub mod scheduler;

use plans::{Sink, Source};
use std::sync::Arc;

pub trait PhysicalOperator: Source + Sink {}

#[derive(Debug)]
pub struct Pipeline {
    /// Destination for all resulting record batches.
    pub destination: Arc<dyn Sink>,

    /// Linked operators for the pipeline.
    // TODO: This also includes data sources (stuff we're not pushing to).
    // Currently unsure how we want to split this up.
    pub operators: Vec<LinkedOperator>,
}

impl Pipeline {
    /// Create a new pipeline with no operators.
    pub fn new_empty(destination: Arc<dyn Sink>) -> Self {
        Pipeline {
            destination,
            operators: Vec::new(),
        }
    }

    /// Push a linked operator into the pipeline, returning that operator's
    /// index in the pipeline.
    pub fn push(&mut self, operator: LinkedOperator) -> usize {
        let idx = self.operators.len();
        self.operators.push(operator);
        idx
    }
}

/// Where to send an operator's output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    /// Send to another operator in the pipeline.
    Operator {
        /// Which operator we're sending to.
        operator: usize,

        /// For operators that accept batches from multiple children (joins),
        /// indicate which child we're sending from.
        child: usize,
    },
    /// Send to the pipelines configured output.
    PipelineOutput,
}

/// An operator in the pipeline that will send its output to some destination.
#[derive(Debug)]
pub struct LinkedOperator {
    pub operator: Arc<dyn PhysicalOperator>,
    pub dest: Destination,
}
