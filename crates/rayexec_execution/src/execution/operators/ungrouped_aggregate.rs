use std::fmt::Debug;
use std::sync::Arc;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use super::hash_aggregate::distinct::DistinctGroupedStates;
use super::hash_aggregate::hash_table::GroupAddress;
use super::{
    ExecutableOperator,
    ExecutionStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
    UnaryInputStates,
};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::operators::InputOutputStates;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalAggregateExpression;
use crate::functions::aggregate::states::AggregateGroupStates;
use crate::proto::DatabaseProtoConv;

#[derive(Debug)]
pub enum UngroupedAggregatePartitionState {
    Aggregating {
        /// Binary data containing values for each aggregate.
        values: Vec<Vec<u8>>,
    },
}

#[derive(Debug)]
pub struct UngroupedAggregateOperatorState {
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
struct OperatorStateInner {}

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate {
    /// Aggregates we're computing.
    ///
    /// Used to create the initial states.
    aggregates: Vec<PhysicalAggregateExpression>,
}

impl PhysicalUngroupedAggregate {
    pub fn new(aggregates: Vec<PhysicalAggregateExpression>) -> Self {
        PhysicalUngroupedAggregate { aggregates }
    }
}

impl ExecutableOperator for PhysicalUngroupedAggregate {
    type States = UnaryInputStates;
}

impl Explainable for PhysicalUngroupedAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalUngroupedAggregate")
    }
}
