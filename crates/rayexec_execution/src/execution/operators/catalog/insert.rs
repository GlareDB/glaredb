use std::sync::Arc;
use std::task::Context;

use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::catalog::entry::CatalogEntry;
use crate::execution::operators::{
    BaseOperator,
    ExecuteOperator,
    ExecutionProperties,
    PollExecute,
    PollFinalize,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::storage::datatable::{DataTable, DataTableAppendState};
use crate::storage::storage_manager::StorageManager;

#[derive(Debug)]
pub struct InsertOperatorState {
    datatable: Arc<DataTable>,
}

#[derive(Debug)]
pub struct InsertPartitionState {
    finished: bool,
    count: i64,
    state: DataTableAppendState,
}

#[derive(Debug)]
pub struct PhysicalInsert {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) entry: Arc<CatalogEntry>,
}

impl BaseOperator for PhysicalInsert {
    type OperatorState = InsertOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        let ent = self.entry.try_as_table_entry()?;
        let datatable = self.storage.get_table(ent.storage_id)?;

        Ok(InsertOperatorState { datatable })
    }

    fn output_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }
}

impl ExecuteOperator for PhysicalInsert {
    type PartitionExecuteState = InsertPartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let states = (0..partitions)
            .map(|_| InsertPartitionState {
                finished: false,
                count: 0,
                state: operator_state.datatable.init_append_state(),
            })
            .collect();

        Ok(states)
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        if state.finished {
            output.arrays[0].set_value(0, &state.count.into())?;
            output.set_num_rows(1)?;
            return Ok(PollExecute::Exhausted);
        }

        state.count += input.num_rows() as i64;
        operator_state
            .datatable
            .append_batch(&mut state.state, input)?;

        Ok(PollExecute::NeedsMore)
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        operator_state.datatable.flush(&mut state.state)?;
        state.finished = true;

        Ok(PollFinalize::NeedsDrain)
    }
}

impl Explainable for PhysicalInsert {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert")
    }
}
