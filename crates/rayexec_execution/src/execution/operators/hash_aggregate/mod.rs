mod aggregate_hash_table;
mod grouping_set_hash_table;

use std::collections::BTreeSet;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{ExecuteInOutState, PollExecute, PollFinalize, UnaryInputStates};
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::ScalarValue;
use crate::database::DatabaseContext;
use crate::execution::operators::{ExecutableOperator, OperatorState, PartitionState};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::{PhysicalAggregateExpression, PhysicalScalarExpression};
use crate::logical::logical_aggregate::GroupingFunction;

#[derive(Debug)]
pub struct Aggregates {
    /// Expressions making up the groups.
    pub groups: Vec<PhysicalScalarExpression>,
    /// GROUPING functions.
    pub grouping_functions: Vec<GroupingFunction>,
    /// Aggregate expressions.
    pub aggregates: Vec<PhysicalAggregateExpression>,
}

#[derive(Debug)]
pub enum HashAggregatePartitionState {}

#[derive(Debug)]
pub struct HashAggregateOperatorState {
    inner: Mutex<HashAggregateOperatoreStateInner>,
}

#[derive(Debug)]
struct HashAggregateOperatoreStateInner {}

/// Compute aggregates over input batches.
///
/// Output batch columns will include the computed aggregate, followed by the
/// group by columns.
#[derive(Debug)]
pub struct PhysicalHashAggregate {
    /// All grouping sets we're grouping by.
    grouping_sets: Vec<BTreeSet<usize>>,
}

impl PhysicalHashAggregate {}

impl ExecutableOperator for PhysicalHashAggregate {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        unimplemented!()
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashAggregate")
    }
}
