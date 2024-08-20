use crate::{
    database::{catalog::CatalogTx, catalog_entry::CatalogEntry, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    proto::DatabaseProtoConv,
    storage::table_storage::DataTableInsert,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{
    sync::Arc,
    task::{Context, Waker},
};

use super::{
    ExecutableOperator, ExecutionStates, InputOutputStates, OperatorState, PartitionState,
    PollFinalize, PollPull, PollPush,
};

#[derive(Debug)]
pub struct InsertPartitionState {
    insert: Box<dyn DataTableInsert>,
    finished: bool,

    // TODO: I'm not exactly sure where I want this. The idea is that inserts
    // are essentially async functions (e.g. `async fn insert_postgres(...)`)
    // and our contexts will be provided during the function call. But I don't
    // know if we want to rely solely on that.
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalInsert {
    catalog: String,
    schema: String,
    table: Arc<CatalogEntry>,
}

impl PhysicalInsert {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: Arc<CatalogEntry>,
    ) -> Self {
        PhysicalInsert {
            catalog: catalog.into(),
            schema: schema.into(),
            table,
        }
    }
}

impl ExecutableOperator for PhysicalInsert {
    fn create_states(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let num_partitions = partitions[0];

        // TODO: Placeholder.
        let _tx = CatalogTx::new();

        let database = context.get_database(&self.catalog)?;
        let data_table = database
            .table_storage
            .as_ref()
            .ok_or_else(|| RayexecError::new("Missing table storage for insert"))?
            .data_table(&self.schema, &self.table)?;

        // TODO: Pass constraints, on conflict
        let inserts = data_table.insert(num_partitions)?;

        let states = inserts
            .into_iter()
            .map(|insert| {
                PartitionState::Insert(InsertPartitionState {
                    insert,
                    finished: false,
                    pull_waker: None,
                })
            })
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states,
            },
        })
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::Insert(state) => {
                let poll = state.insert.poll_push(cx, batch)?;

                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(poll)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::Insert(state) => {
                if let PollFinalize::Pending = state.insert.poll_finalize_push(cx)? {
                    return Ok(PollFinalize::Pending);
                }

                state.finished = true;
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollFinalize::Finalized)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Insert(state) => {
                if state.finished {
                    Ok(PollPull::Exhausted)
                } else {
                    state.pull_waker = Some(cx.waker().clone());
                    Ok(PollPull::Pending)
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalInsert {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert").with_value("table", &self.table.name)
    }
}

impl DatabaseProtoConv for PhysicalInsert {
    type ProtoType = rayexec_proto::generated::execution::PhysicalInsert;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     catalog: self.catalog.clone(),
        //     schema: self.schema.clone(),
        //     table: Some(self.table.to_proto()?),
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     catalog: proto.catalog,
        //     schema: proto.schema,
        //     table: TableEntry::from_proto(proto.table.required("table")?)?,
        // })
    }
}