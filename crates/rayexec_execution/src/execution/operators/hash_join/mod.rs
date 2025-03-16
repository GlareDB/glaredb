use std::task::Context;

use join_hash_table::{BuildState, HashJoinCondition, JoinHashTable};
use rayexec_error::Result;

use super::{
    BaseOperator,
    ExecuteOperator,
    ExecutionProperties,
    PollExecute,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::logical_join::JoinType;

mod hash_table_scan;
mod join_hash_table;

#[derive(Debug)]
pub struct HashJoinOperatorState {
    _table: JoinHashTable,
}

#[derive(Debug)]
pub struct HashJoinPartitionBuildState {
    _build_state: BuildState,
}

#[derive(Debug)]
pub struct HashJoinPartitionProbeState {}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    pub(crate) _join_type: JoinType,
    pub(crate) _left_types: Vec<DataType>,
    pub(crate) _right_types: Vec<DataType>,
    pub(crate) _conditions: Vec<HashJoinCondition>,
}

impl BaseOperator for PhysicalHashJoin {
    type OperatorState = HashJoinOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        unimplemented!()
    }

    fn output_types(&self) -> &[DataType] {
        unimplemented!()
    }
}

impl ExecuteOperator for PhysicalHashJoin {
    type PartitionExecuteState = HashJoinPartitionProbeState;

    fn create_partition_execute_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        _partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionExecuteState,
        _input: &mut Batch,
        _output: &mut Batch,
    ) -> Result<PollExecute> {
        unimplemented!()
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl PushOperator for PhysicalHashJoin {
    type PartitionPushState = HashJoinPartitionBuildState;

    fn create_partition_push_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        _partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        unimplemented!()
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionPushState,
        _input: &mut Batch,
    ) -> Result<PollPush> {
        unimplemented!()
        // operator_state
        //     .table
        //     .collect_build(&mut state.build_state, input)?;
        // Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashJoin")
    }
}
