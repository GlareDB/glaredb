use crate::execution::query_graph::sink::{PartitionSink, QuerySink};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::Context;

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct QuerySinkPartitionState {
    sink: Box<dyn PartitionSink>,
}

impl QuerySinkPartitionState {
    pub fn new(sink: Box<dyn PartitionSink>) -> Self {
        QuerySinkPartitionState { sink }
    }
}

/// Wrapper around a query sink to implement the physical operator trait.
#[derive(Debug)]
pub struct PhysicalQuerySink;

impl PhysicalOperator for PhysicalQuerySink {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::QuerySink(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.sink.poll_push(cx, batch)
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        let state = match partition_state {
            PartitionState::QuerySink(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.sink.finalize_push()
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        Err(RayexecError::new("Query sink cannot be pulled from"))
    }
}
