//! Planning and execution of physical plans.

pub mod chain;
pub mod planner;
pub mod plans;
pub mod scheduler;

use crossbeam::channel::Sender;
use plans::{Sink, Source};
use std::sync::Arc;

use crate::engine::modify::Modification;

use self::chain::OperatorChain;

/// A per-task context used during execution.
///
/// Cheaply cloneable to allow each task a complete copy of the context.
//
// TODO: Store execution timings on this. Then the outermost EXPLAIN node will
// be able to pull those timings off of this to expose them to the user.
//
// TODO: I don't think this is the cleanest solution right now as it's unclear
// what this will be used for other than being able to apply modifications to a
// session.
//
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// A channel for sending session modifications to the session.
    ///
    /// Currently this is used to allow for updating session variables, but can
    /// be used for catalog updates, statistics propogation, and transaction
    /// status updates.
    ///
    /// May be None if modifying the session shouldn't happen. This will result
    /// in an error when a plan like SET tries to make a change.
    pub modifications: Option<Sender<Modification>>,
}

#[derive(Debug)]
pub struct Pipeline {
    /// The operator chains that make up this pipeline.
    pub chains: Vec<Arc<OperatorChain>>,
}
