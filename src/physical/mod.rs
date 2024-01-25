//! Planning and execution of physical plans.

pub mod plans;
pub mod scheduler;

use plans::{Sink, Source};
use std::sync::Arc;

pub trait Operator: Source + Sink {}

#[derive(Debug)]
pub struct Pipeline {
    /// Destination for all resulting record batches.
    pub destination: Box<dyn Sink>,

    /// Linked operators for the pipeline.
    // TODO: This also includes data sources (stuff we're not pushing to).
    // Currently unsure how we want to split this up.
    pub operators: Vec<LinkedOperator>,
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
    pub operator: Box<dyn Operator>,
    pub dest: Destination,
}
