use crate::{
    database::{
        create::CreateTableInfo,
        ddl::CreateFut,
        table::{DataTable, DataTableInsert},
        DatabaseContext,
    },
    planner::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Poll, Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub enum CreateTableAsPartitionState {
    /// We're creating the table.
    Creating {
        partition_idx: usize,
        num_insert_partitions: usize,
        create: Box<dyn CreateFut<Output = Box<dyn DataTable>>>,
    },

    /// We're inserting into the table.
    Inserting {
        partition_idx: usize,
        insert: Option<Box<dyn DataTableInsert>>,
        finished: bool,
    },
}

#[derive(Debug)]
pub struct CreateTableAsOperatorState {
    shared: Mutex<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    inserts: Vec<Option<Box<dyn DataTableInsert>>>,
    push_wakers: Vec<Option<Waker>>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct PhysicalCreateTableAs {
    catalog: String,
    schema: String,
    info: CreateTableInfo,
}

impl PhysicalCreateTableAs {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        info: CreateTableInfo,
    ) -> Self {
        PhysicalCreateTableAs {
            catalog: catalog.into(),
            schema: schema.into(),
            info,
        }
    }

    pub fn try_create_state(
        &self,
        _context: &DatabaseContext,
    ) -> Result<CreateTableAsPartitionState> {
        // TODO: Placeholder.
        // let tx = CatalogTx::new();

        // let create = context
        //     .get_catalog(&self.catalog)?
        //     .catalog_modifier(&tx)?
        //     .create_table(&self.schema, self.info.clone())?;

        unimplemented!()
        // Ok(CreateTablePartitionState { create })
    }

    /// Try to create the table if this partition is the one responsible for it.
    ///
    /// If the table was successfully created, insert objects will be placed
    /// into the global state for other partitions to use.
    ///
    /// Partitions that are already in the inserting stage will return a a
    /// `Poll::Ready(Ok(()))`.
    fn poll_maybe_create(
        cx: &mut Context,
        state: &mut CreateTableAsPartitionState,
        operator_state: &CreateTableAsOperatorState,
    ) -> Poll<Result<()>> {
        if let CreateTableAsPartitionState::Creating {
            partition_idx,
            num_insert_partitions,
            create,
        } = state
        {
            let table = match create.poll_create(cx) {
                Poll::Ready(Ok(table)) => table,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            let inserts = table.insert(*num_insert_partitions)?;

            let mut shared = operator_state.shared.lock();

            shared.inserts = inserts.into_iter().map(Some).collect();
            let insert = shared.inserts[*partition_idx].take();

            *state = CreateTableAsPartitionState::Inserting {
                partition_idx: *partition_idx,
                insert,
                finished: false,
            };

            // Wake up other insert partitions.
            for waker in shared.push_wakers.iter_mut() {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl PhysicalOperator for PhysicalCreateTableAs {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::CreateTableAs(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::CreateTableAs(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match Self::poll_maybe_create(cx, state, operator_state) {
            Poll::Pending => return Ok(PollPush::Pending(batch)),
            Poll::Ready(Err(e)) => return Err(e),
            Poll::Ready(Ok(())) => (), // Continue to try to insert into the table.
        };

        match state {
            CreateTableAsPartitionState::Inserting {
                partition_idx,
                insert,
                ..
            } => {
                // Try to get the insert object from the global state if we
                // don't already have it.
                if insert.is_none() {
                    let mut shared = operator_state.shared.lock();
                    *insert = shared.inserts[*partition_idx].take();

                    // Table still needs to be created, register our waker for
                    // later wake up once it's created.
                    if insert.is_none() {
                        shared.push_wakers[*partition_idx] = Some(cx.waker().clone());
                        return Ok(PollPush::Pending(batch));
                    }
                }

                // Push it.
                insert
                    .as_mut()
                    .expect("insert to exist")
                    .poll_push(cx, batch)
            }
            other => panic!("state should be 'inserting' by here, got {other:?}"),
        }
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
        // match partition_state {
        //     PartitionState::CreateTable(state) => match state.create.poll_create(cx) {
        //         Poll::Ready(Ok(_)) => Ok(PollPull::Exhausted),
        //         Poll::Ready(Err(e)) => Err(e),
        //         Poll::Pending => Ok(PollPull::Pending),
        //     },
        //     other => panic!("invalid partition state: {other:?}"),
        // }
    }
}

impl Explainable for PhysicalCreateTableAs {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTableAs").with_value("table", &self.info.name)
    }
}
