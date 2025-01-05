use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::{RayexecError, Result};

use super::util::futures::make_static;
use super::{
    ExecutableOperator,
    ExecutionStates2,
    InputOutputStates2,
    OperatorState,
    PartitionState,
    PollFinalize2,
    PollPull2,
    PollPush2,
};
use crate::arrays::batch::Batch2;
use crate::database::catalog::CatalogTx;
use crate::database::catalog_entry::CatalogEntry;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;
use crate::storage::table_storage::{DataTableScan, Projections};

pub struct ScanPartitionState {
    scan: Box<dyn DataTableScan>,
    /// In progress pull we're working on.
    future: Option<BoxFuture<'static, Result<Option<Batch2>>>>,
}

impl fmt::Debug for ScanPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanPartitionState").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct PhysicalScan {
    catalog: String,
    schema: String,
    table: Arc<CatalogEntry>,
    projections: Projections,
}

impl PhysicalScan {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: Arc<CatalogEntry>,
        projections: Projections,
    ) -> Self {
        PhysicalScan {
            catalog: catalog.into(),
            schema: schema.into(),
            table,
            projections,
        }
    }
}

impl ExecutableOperator for PhysicalScan {
    fn create_states2(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates2> {
        // TODO: Placeholder for now. Transaction info should probably go on the
        // operator.
        let _tx = CatalogTx::new();

        let database = context.get_database(&self.catalog)?;
        let data_table = database
            .table_storage
            .as_ref()
            .ok_or_else(|| RayexecError::new("Missing table storage for scan"))?
            .data_table(&self.schema, &self.table)?;

        // TODO: Pushdown projections, filters
        let scans = data_table.scan(self.projections.clone(), partitions[0])?;

        let states = scans
            .into_iter()
            .map(|scan| PartitionState::Scan(ScanPartitionState { scan, future: None }))
            .collect();

        Ok(ExecutionStates2 {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates2::OneToOne {
                partition_states: states,
            },
        })
    }

    fn poll_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch2,
    ) -> Result<PollPush2> {
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn poll_finalize_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize2> {
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn poll_pull2(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull2> {
        match partition_state {
            PartitionState::Scan(state) => {
                if let Some(future) = &mut state.future {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(Some(batch))) => {
                            state.future = None; // Future complete, next pull with create a new one.
                            return Ok(PollPull2::Computed(batch.into()));
                        }
                        Poll::Ready(Ok(None)) => return Ok(PollPull2::Exhausted),
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull2::Pending),
                    }
                }

                let mut future = state.scan.pull();
                match future.poll_unpin(cx) {
                    Poll::Ready(Ok(Some(batch))) => Ok(PollPull2::Computed(batch.into())),
                    Poll::Ready(Ok(None)) => Ok(PollPull2::Exhausted),
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => {
                        // SAFETY: Scan lives on the partition state and
                        // outlives this future.
                        state.future = Some(unsafe { make_static(future) });
                        Ok(PollPull2::Pending)
                    }
                }
            }

            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Scan").with_value("table", &self.table.name)
    }
}

impl DatabaseProtoConv for PhysicalScan {
    type ProtoType = rayexec_proto::generated::execution::PhysicalScan;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            table: Some(self.table.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        // TODO: https://github.com/GlareDB/rayexec/issues/278
        unimplemented!()
        // Ok(Self {
        //     catalog: proto.catalog,
        //     schema: proto.schema,
        //     table: Arc::new(DatabaseProtoConv::from_proto_ctx(
        //         proto.table.required("table")?,
        //         context,
        //     )?),
        // })
    }
}
