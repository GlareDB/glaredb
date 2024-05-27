//! Implementations of physical operators in an execution pipeline.

pub mod aggregate;
pub mod empty;
pub mod filter;
pub mod join;
pub mod limit;
pub mod project;
pub mod query_sink;
pub mod repartition;
pub mod simple;
pub mod sort;
pub mod values;

mod util;

#[cfg(test)]
mod test_util;

use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt::Debug;
use std::task::Context;

use crate::planner::explainable::Explainable;

use self::aggregate::hash_aggregate::{HashAggregateOperatorState, HashAggregatePartitionState};
use self::empty::EmptyPartitionState;
use self::join::hash_join::{
    HashJoinBuildPartitionState, HashJoinOperatorState, HashJoinProbePartitionState,
};
use self::join::nl_join::{
    NestedLoopJoinBuildPartitionState, NestedLoopJoinOperatorState,
    NestedLoopJoinProbePartitionState,
};
use self::limit::LimitPartitionState;
use self::query_sink::QuerySinkPartitionState;
use self::repartition::hash::{HashRepartitionOperatorState, HashRepartitionPartitionState};
use self::repartition::round_robin::{
    RoundRobinOperatorState, RoundRobinPullPartitionState, RoundRobinPushPartitionState,
};
use self::simple::SimplePartitionState;
use self::sort::local_sort::LocalSortPartitionState;
use self::sort::merge_sorted::{
    MergeSortedOperatorState, MergeSortedPullPartitionState, MergeSortedPushPartitionState,
};
use self::values::ValuesPartitionState;

/// States local to a partition within a single operator.
// Current size: 200 bytes
#[derive(Debug)]
pub enum PartitionState {
    HashAggregate(HashAggregatePartitionState),
    NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState),
    NestedLoopJoinProbe(NestedLoopJoinProbePartitionState),
    HashJoinBuild(HashJoinBuildPartitionState),
    HashJoinProbe(HashJoinProbePartitionState),
    Values(ValuesPartitionState),
    QuerySink(QuerySinkPartitionState),
    RoundRobinPush(RoundRobinPushPartitionState),
    RoundRobinPull(RoundRobinPullPartitionState),
    HashRepartition(HashRepartitionPartitionState),
    MergeSortedPush(MergeSortedPushPartitionState),
    MergeSortedPull(MergeSortedPullPartitionState),
    LocalSort(LocalSortPartitionState),
    Limit(LimitPartitionState),
    Simple(SimplePartitionState),
    Empty(EmptyPartitionState),
    None,
}

/// A global state across all partitions in an operator.
// Current size: 112 bytes
#[derive(Debug)]
pub enum OperatorState {
    HashAggregate(HashAggregateOperatorState),
    NestedLoopJoin(NestedLoopJoinOperatorState),
    HashJoin(HashJoinOperatorState),
    RoundRobin(RoundRobinOperatorState),
    HashRepartition(HashRepartitionOperatorState),
    MergeSorted(MergeSortedOperatorState),
    None,
}

/// Result of a push to an operator.
///
/// An operator may not be ready to accept input either because it's waiting on
/// something else to complete (e.g. the right side of a join needs to the left
/// side to complete first) or some internal buffer is full.
#[derive(Debug, PartialEq)]
pub enum PollPush {
    /// Batch was successfully pushed.
    Pushed,

    /// Batch could not be processed right now.
    ///
    /// A waker will be registered for a later wakeup. This same batch should be
    /// pushed at that time.
    Pending(Batch),

    /// This operator requires no more input.
    ///
    /// `finalize_push` for the operator should _not_ be called.
    Break,

    /// Batch was successfully pushed, but the operator needs more input before
    /// it can start producing output
    NeedsMore,
}

/// Result of a pull from a Source.
#[derive(Debug, PartialEq)]
pub enum PollPull {
    /// Successfully received a data batch.
    Batch(Batch),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The operator has been exhausted for this partition.
    Exhausted,
}

pub trait PhysicalOperator: Sync + Send + Debug + Explainable {
    /// Try to push a batch for this partition.
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush>;

    /// Finalize pushing to partition.
    ///
    /// This indicates the operator will receive no more input for a given
    /// partition, allowing the operator to execution some finalization logic.
    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()>;

    /// Try to pull a batch for this partition.
    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull>;
}
