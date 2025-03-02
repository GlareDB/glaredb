use std::task::Context;

use rayexec_error::Result;

// use super::join_hash_table::{HashTableScanState, JoinHashTable};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
// use crate::execution::operators::join::empty_output_on_empty_build;
use crate::execution::operators::{
    BinaryInputStates,
    ExecutableOperator,
    ExecuteInOut,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::functions::scalar::PlannedScalarFunction;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct HashJoinOperatorState {}

#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left: PhysicalScalarExpression,
    pub right: PhysicalScalarExpression,
    pub function: PlannedScalarFunction,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    pub(crate) join_type: JoinType,
    pub(crate) left_types: Vec<DataType>,
    pub(crate) right_types: Vec<DataType>,
    pub(crate) conditions: Vec<JoinCondition>,
    /// Indices into `conditions` for equality conditions.
    pub(crate) eq_conditions: Vec<usize>,
}

impl ExecutableOperator for PhysicalHashJoin {
    type States = BinaryInputStates;

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Self::States> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOut,
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

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashJoin")
    }
}

#[derive(Debug)]
pub struct HashJoinBuildPartitionState {}

#[derive(Debug)]
pub struct HashJoinProbePartitionState {
    probe_eval: ExpressionEvaluator,
    rhs_keys: Batch,
    // hash_table: Option<Arc<JoinHashTable>>,
    // scan_state: HashTableScanState,
}

impl HashJoinProbePartitionState {
    fn probe(
        &mut self,
        join_type: JoinType,
        operator_state: &HashJoinOperatorState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        // let hash_table = self
        //     .hash_table
        //     .as_ref()
        //     .expect("hash table to have been built");

        // if hash_table.row_count() == 0 {
        //     if empty_output_on_empty_build(join_type) {
        //         output.set_num_rows(0)?;
        //         return Ok(PollExecute::Exhausted);
        //     } else {
        //         unimplemented!()
        //     }
        // }

        // if self.scan_state.is_none() {
        //     unimplemented!()
        // }

        // let scan_state = self.scan_state.as_mut().expect("scan state to be set");

        unimplemented!()
    }
}
