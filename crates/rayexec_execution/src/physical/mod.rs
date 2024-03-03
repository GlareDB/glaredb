//! Planning and execution of physical plans.

pub mod chain;
pub mod datasource;
pub mod planner;
pub mod plans;
pub mod scheduler;

use plans::{Sink2, Source2};
use std::fmt;
use std::sync::Arc;

use crate::planner::explainable::{ExplainConfig, Explainable};

pub trait PhysicalOperator2: Source2 + Sink2 + Explainable {}

pub struct Pipeline2 {
    /// Destination for all resulting record batches.
    pub destination: Box<dyn Sink2>,

    /// Linked operators for the pipeline.
    // TODO: This also includes data sources (stuff we're not pushing to).
    // Currently unsure how we want to split this up.
    pub operators: Vec<LinkedOperator>,
}

impl Pipeline2 {
    /// Create a new pipeline with no operators.
    pub fn new_empty(destination: Box<dyn Sink2>) -> Self {
        Pipeline2 {
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

impl fmt::Debug for Pipeline2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, operator) in self.operators.iter().enumerate() {
            if idx != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{idx}: {operator:?}")?;
        }
        Ok(())
    }
}

/// Where to send an operator's output.
#[derive(Clone, Copy, PartialEq, Eq)]
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

impl fmt::Debug for Destination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operator { operator, child } => f
                .debug_tuple("Destination")
                .field(operator)
                .field(child)
                .finish(),
            Self::PipelineOutput => f
                .debug_tuple("Destination")
                .field(&"pipeline output")
                .finish(),
        }
    }
}

/// An operator in the pipeline that will send its output to some destination.
pub struct LinkedOperator {
    pub operator: Arc<dyn PhysicalOperator2>,
    pub dest: Destination,
}

impl fmt::Debug for LinkedOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let conf = ExplainConfig { verbose: true };
        let explain = self.operator.explain_entry(conf);
        write!(f, "{explain} {:?}", self.dest)
    }
}
