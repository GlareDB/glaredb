mod hash_table;

use std::task::Context;

use glaredb_error::Result;
use hash_table::{HashJoinCondition, HashTableBuildPartitionState, JoinHashTable};

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
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::comparison_expr::ComparisonOperator;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct HashJoinOperatorState {
    _table: JoinHashTable,
}

#[derive(Debug)]
pub struct HashJoinPartitionBuildState {
    _build_state: HashTableBuildPartitionState,
}

#[derive(Debug)]
pub struct HashJoinPartitionProbeState {}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    pub(crate) join_type: JoinType,
    pub(crate) left_types: Vec<DataType>,
    pub(crate) right_types: Vec<DataType>,
    pub(crate) output_types: Vec<DataType>,
    pub(crate) conditions: Vec<HashJoinCondition>,
}

impl PhysicalHashJoin {
    pub fn new(
        join_type: JoinType,
        left_types: impl IntoIterator<Item = DataType>,
        right_types: impl IntoIterator<Item = DataType>,
    ) -> Result<Self> {
        unimplemented!()
    }
}

impl BaseOperator for PhysicalHashJoin {
    const OPERATOR_NAME: &str = "HashJoin";

    type OperatorState = HashJoinOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        unimplemented!()
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
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
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new(Self::OPERATOR_NAME, conf).build()
    }
}
