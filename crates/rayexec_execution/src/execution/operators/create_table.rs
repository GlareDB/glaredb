use crate::{
    database::{
        catalog::CatalogTx,
        create::CreateTableInfo,
        table::{DataTable, DataTableInsert},
        DatabaseContext,
    },
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    proto::DatabaseProtoConv,
};
use futures::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;
use std::{fmt, task::Waker};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

use super::{
    ExecutableOperator, ExecutionStates, InputOutputStates, OperatorState, PartitionState,
    PollFinalize, PollPull, PollPush,
};

pub enum CreateTablePartitionState {
    /// State when we're creating the table.
    Creating {
        /// Future for creating the table.
        create: BoxFuture<'static, Result<Box<dyn DataTable>>>,

        /// After creation, how many insert partitions we'll want to make.
        insert_partitions: usize,

        /// Index of this partition.
        partition_idx: usize,

        pull_waker: Option<Waker>,
    },

    /// State when we're inserting into the new table.
    Inserting {
        /// Insert into the new table.
        ///
        /// If None, global state should be checked.
        insert: Option<Box<dyn DataTableInsert>>,

        /// Index of this partition.
        partition_idx: usize,

        /// If we're done inserting.
        finished: bool,

        pull_waker: Option<Waker>,
    },
}

impl fmt::Debug for CreateTablePartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTablePartitionState").finish()
    }
}

#[derive(Debug)]
pub struct CreateTableOperatorState {
    shared: Mutex<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    inserts: Vec<Option<Box<dyn DataTableInsert>>>,
    push_wakers: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalCreateTable {
    catalog: String,
    schema: String,
    info: CreateTableInfo,
    is_ctas: bool,
}

impl PhysicalCreateTable {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        info: CreateTableInfo,
        is_ctas: bool,
    ) -> Self {
        PhysicalCreateTable {
            catalog: catalog.into(),
            schema: schema.into(),
            info,
            is_ctas,
        }
    }
}

impl ExecutableOperator for PhysicalCreateTable {
    fn create_states(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let insert_partitions = partitions[0];

        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let catalog = context.get_catalog(&self.catalog)?.catalog_modifier(&tx)?;
        let create = catalog.create_table(&self.schema, self.info.clone());

        // First partition will be responsible for the create.
        let mut states = vec![CreateTablePartitionState::Creating {
            create,
            insert_partitions,
            partition_idx: 0,
            pull_waker: None,
        }];

        // Rest of the partitions will start on insert, waiting until the first
        // partition completes.
        states.extend(
            (1..insert_partitions).map(|idx| CreateTablePartitionState::Inserting {
                insert: None,
                partition_idx: idx,
                finished: !self.is_ctas, // If we're a normal create table, mark all inserts as complete.
                pull_waker: None,
            }),
        );

        let operator_state = CreateTableOperatorState {
            shared: Mutex::new(SharedState {
                inserts: (0..insert_partitions).map(|_| None).collect(),
                push_wakers: vec![None; insert_partitions],
            }),
        };

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::CreateTable(operator_state)),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states
                    .into_iter()
                    .map(PartitionState::CreateTable)
                    .collect(),
            },
        })
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::CreateTable(CreateTablePartitionState::Creating {
                create,
                insert_partitions,
                partition_idx,
                pull_waker,
            }) => match create.poll_unpin(cx) {
                Poll::Ready(Ok(table)) => {
                    let insert_partitions = *insert_partitions;
                    let partition_idx = *partition_idx;

                    *partition_state =
                        PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                            insert: None,
                            partition_idx,
                            finished: !self.is_ctas,
                            pull_waker: pull_waker.take(),
                        });

                    if !self.is_ctas {
                        // If we're not a CTAS, we can just skip creating the
                        // table inserts.
                        return Ok(PollPush::Pushed);
                    }

                    let inserts = table.insert(insert_partitions)?;
                    let inserts: Vec<_> = inserts.into_iter().map(Some).collect();

                    let mut shared = match operator_state {
                        OperatorState::CreateTable(state) => state.shared.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    shared.inserts = inserts;

                    for waker in shared.push_wakers.iter_mut() {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }

                    // Continue on, we'll be doing the insert in the below match.
                }
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => return Ok(PollPush::Pending(batch)),
            },
            PartitionState::CreateTable(_) => (), // Fall through to below match.
            other => panic!("invalid partition state: {other:?}"),
        }

        match partition_state {
            PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                insert,
                partition_idx,
                ..
            }) => {
                if insert.is_none() {
                    let mut shared = match operator_state {
                        OperatorState::CreateTable(state) => state.shared.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    if shared.inserts[*partition_idx].is_none() {
                        shared.push_wakers[*partition_idx] = Some(cx.waker().clone());
                        return Ok(PollPush::Pending(batch));
                    }

                    *insert = shared.inserts[*partition_idx].take();
                }

                let insert = insert.as_mut().expect("insert to be Some");
                // Insert will store the context if it returns pending.
                insert.poll_push(cx, batch)
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
            PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                finished,
                pull_waker,
                insert,
                ..
            }) => {
                if let Some(insert) = insert {
                    if let PollFinalize::Pending = insert.poll_finalize_push(cx)? {
                        return Ok(PollFinalize::Pending);
                    }
                }

                *finished = true;
                if let Some(waker) = pull_waker.take() {
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
            PartitionState::CreateTable(CreateTablePartitionState::Inserting {
                finished,
                pull_waker,
                ..
            }) => {
                if *finished {
                    return Ok(PollPull::Exhausted);
                }
                *pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
            PartitionState::CreateTable(CreateTablePartitionState::Creating {
                pull_waker, ..
            }) => {
                *pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalCreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable").with_value("table", &self.info.name)
    }
}

impl DatabaseProtoConv for PhysicalCreateTable {
    type ProtoType = rayexec_proto::generated::execution::PhysicalCreateTable;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            info: Some(self.info.to_proto()?),
            is_ctas: self.is_ctas,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            catalog: proto.catalog,
            schema: proto.schema,
            info: CreateTableInfo::from_proto(proto.info.required("info")?)?,
            is_ctas: proto.is_ctas,
        })
    }
}
