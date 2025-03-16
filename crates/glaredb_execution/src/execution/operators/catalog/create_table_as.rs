use std::sync::Arc;
use std::task::Context;

use glaredb_error::Result;
use parking_lot::Mutex;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::catalog::create::CreateTableInfo;
use crate::catalog::memory::MemorySchema;
use crate::catalog::Schema;
use crate::config::session::DEFAULT_BATCH_SIZE;
use crate::execution::operators::{
    BaseOperator,
    ExecuteOperator,
    ExecutionProperties,
    PollExecute,
    PollFinalize,
};
use crate::execution::partition_wakers::PartitionWakers;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::storage::datatable::{DataTable, DataTableAppendState};
use crate::storage::storage_manager::StorageManager;

#[derive(Debug)]
pub struct CreateTableAsOperatorState {
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
struct OperatorStateInner {
    /// A partition is in the process of creating the datatable. All other
    /// partitions should return pending.
    creating: bool,
    /// The datatable created.
    table: Option<Arc<DataTable>>,
    /// Wakers for other partitions waiting on the table to create.
    wakers: PartitionWakers,
}

#[derive(Debug)]
pub struct CreateTableAsPartitionState {
    partition_idx: usize,
    finished: bool,
    inner: Option<PartitionStateInner>,
    count: i64,
}

#[derive(Debug)]
struct PartitionStateInner {
    table: Arc<DataTable>,
    state: DataTableAppendState,
}

#[derive(Debug)]
pub struct PhysicalCreateTableAs {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) schema: Arc<MemorySchema>,
    pub(crate) info: CreateTableInfo,
}

impl BaseOperator for PhysicalCreateTableAs {
    type OperatorState = CreateTableAsOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(CreateTableAsOperatorState {
            inner: Mutex::new(OperatorStateInner {
                creating: false,
                table: None,
                wakers: PartitionWakers::empty(), // Init when creating partition states.
            }),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }
}

impl ExecuteOperator for PhysicalCreateTableAs {
    type PartitionExecuteState = CreateTableAsPartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let mut inner = operator_state.inner.lock();
        inner.wakers.init_for_partitions(partitions);

        let states = (0..partitions)
            .map(|partition_idx| CreateTableAsPartitionState {
                partition_idx,
                finished: false,
                inner: None,
                count: 0,
            })
            .collect();

        Ok(states)
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
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

        if state.inner.is_none() {
            // Need to check operator state to see if table needs to be created/
            // has been created.
            let mut op_state = operator_state.inner.lock();
            if op_state.creating && op_state.table.is_none() {
                // Some other partition currently creating the table, come back
                // later.
                op_state.wakers.store(cx.waker(), state.partition_idx);
                return Ok(PollExecute::Pending);
            }

            if !op_state.creating && op_state.table.is_none() {
                // We're the first partition here. Go ahead and create the
                // table.
                op_state.creating = true;
                std::mem::drop(op_state);

                // TODO: Same as CreateTable, figure out how we want to
                // configure these.
                let datatable = Arc::new(DataTable::new(
                    self.info.columns.iter().map(|f| f.datatype.clone()),
                    16,
                    DEFAULT_BATCH_SIZE,
                ));
                let storage_id = self.storage.insert_table(datatable.clone())?;
                self.schema.create_table(&self.info, storage_id)?;

                op_state = operator_state.inner.lock();
                op_state.creating = false;
                op_state.table = Some(datatable);

                // Wake up pending partitions.
                op_state.wakers.wake_all();

                // Continue on..
            }

            // Clone table into partiton state.
            let datatable = op_state
                .table
                .as_ref()
                .cloned()
                .expect("datatable to exist");
            std::mem::drop(op_state);

            let append_state = datatable.init_append_state();
            state.inner = Some(PartitionStateInner {
                table: datatable,
                state: append_state,
            })
        }

        // Inner should be Some now.
        let inner = state
            .inner
            .as_mut()
            .expect("inner partition state to exist");

        state.count += input.num_rows() as i64;
        inner.table.append_batch(&mut inner.state, input)?;

        Ok(PollExecute::NeedsMore)
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        // May be None if this partition didn't actually try to insert anything
        // into the table.
        if let Some(inner) = &mut state.inner {
            inner.table.flush(&mut inner.state)?;
        }
        state.finished = true;

        Ok(PollFinalize::NeedsDrain)
    }
}

impl Explainable for PhysicalCreateTableAs {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTableAs")
    }
}
