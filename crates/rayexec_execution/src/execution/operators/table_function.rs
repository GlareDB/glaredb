use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::{RayexecError, Result};

use super::util::futures::make_static;
use super::{
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::functions::table::{PlannedTableFunction, TableFunctionImpl};
use crate::proto::DatabaseProtoConv;
use crate::storage::table_storage::{DataTableScan, Projections};

pub struct TableFunctionPartitionState {
    scan_state: Box<dyn DataTableScan>,
    /// In progress pull we're working on.
    future: Option<BoxFuture<'static, Result<Option<Batch>>>>,
}

impl fmt::Debug for TableFunctionPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionPartitionState")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct PhysicalTableFunction {
    function: PlannedTableFunction,
    projections: Projections,
}

impl PhysicalTableFunction {
    pub fn new(function: PlannedTableFunction, projections: Projections) -> Self {
        PhysicalTableFunction {
            function,
            projections,
        }
    }
}

impl ExecutableOperator for PhysicalTableFunction {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let scan_func = match &self.function.function_impl {
            TableFunctionImpl::Scan(scan) => scan,
            _ => {
                return Err(RayexecError::new(format!(
                    "Table function '{}' not a scan function",
                    self.function.function.name()
                )))
            }
        };

        // TODO: Pushdown  filters
        let scans = scan_func.scan(self.projections.clone(), partitions[0])?;

        let states = scans
            .into_iter()
            .map(|scan_state| {
                PartitionState::TableFunction(TableFunctionPartitionState {
                    scan_state,
                    future: None,
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
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        // Could UNNEST be implemented as a table function?
        Err(RayexecError::new("Cannot push to physical table function"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to physical table function"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::TableFunction(state) => {
                if let Some(future) = &mut state.future {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(Some(batch))) => {
                            state.future = None; // Future complete, next pull with create a new one.
                            return Ok(PollPull::Computed(batch.into()));
                        }
                        Poll::Ready(Ok(None)) => return Ok(PollPull::Exhausted),
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull::Pending),
                    }
                }

                let mut future = state.scan_state.pull();
                match future.poll_unpin(cx) {
                    Poll::Ready(Ok(Some(batch))) => Ok(PollPull::Computed(batch.into())),
                    Poll::Ready(Ok(None)) => Ok(PollPull::Exhausted),
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => {
                        // SAFETY: Scan lives on the partition state and
                        // outlives this future.
                        state.future = Some(unsafe { make_static(future) });
                        Ok(PollPull::Pending)
                    }
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalTableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableFunction")
    }
}

impl DatabaseProtoConv for PhysicalTableFunction {
    type ProtoType = rayexec_proto::generated::execution::PhysicalTableFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     function: Some(self.function.to_proto_ctx(context)?),
        //     projections: Some(self.projections.to_proto()?),
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // // TODO: https://github.com/GlareDB/rayexec/issues/278
        // Ok(Self {
        //     function: DatabaseProtoConv::from_proto_ctx(
        //         proto.function.required("function")?,
        //         context,
        //     )?,
        //     projections: ProtoConv::from_proto(proto.projections.required("projections")?)?,
        // })
    }
}
