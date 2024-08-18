use dashmap::DashMap;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use uuid::Uuid;

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    datasource::DataSourceRegistry,
    engine::vars::SessionVars,
    execution::{
        executable::planner::{ExecutablePipelinePlanner, ExecutionConfig, PlanLocationState},
        intermediate::{
            planner::{IntermediateConfig, IntermediatePipelinePlanner},
            IntermediatePipelineGroup, StreamId,
        },
    },
    hybrid::{
        buffer::ServerStreamBuffers,
        client::{HybridPlanResponse, PullStatus},
    },
    logical::{
        binder::{
            bind_data::BindData,
            resolve_hybrid::{HybridContextExtender, HybridResolver},
            BoundStatement,
        },
        planner::plan_statement::PlanContext,
    },
    optimizer::Optimizer,
    runtime::{PipelineExecutor, QueryHandle, Runtime},
};
use std::sync::Arc;

/// Server state for planning and executing user queries on a remote server.
///
/// Keeps no state and very cheap to create. Essentially just encapsulates logic
/// for what should happen on the remote side for hybrid/distributed execution.
#[derive(Debug)]
pub struct ServerState<P: PipelineExecutor, R: Runtime> {
    /// Registered data source implementations.
    registry: Arc<DataSourceRegistry>,

    /// Hybrid execution streams.
    buffers: ServerStreamBuffers,

    pending_pipelines: DashMap<Uuid, PendingPipelineState>,
    executing_pipelines: DashMap<Uuid, Box<dyn QueryHandle>>,

    executor: P,
    _runtime: R,
}

#[derive(Debug)]
struct PendingPipelineState {
    /// Context we used for intial planning, and what we'll be using for
    /// executable pipeline planning.
    context: DatabaseContext,
    /// The pipeline group we'll be turning into executables pipelines.
    group: IntermediatePipelineGroup,
}

impl<P, R> ServerState<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    pub fn new(executor: P, runtime: R, registry: Arc<DataSourceRegistry>) -> Self {
        ServerState {
            registry,
            buffers: ServerStreamBuffers::default(),
            pending_pipelines: DashMap::new(),
            executing_pipelines: DashMap::new(),
            executor,
            _runtime: runtime,
        }
    }

    /// Plans a partially bound query, preparing it for execution.
    ///
    /// An intermediate pipeline group will be returned. This is expected to be
    /// sent back to the client for execution.
    ///
    /// Failing to complete binding (e.g. unable to resolve a table) should
    /// result in an error. Otherwise we can assume that all references are
    /// bound and we can continue with planning for hybrid exec.
    pub async fn plan_partially_bound(
        &self,
        mut context: DatabaseContext,
        stmt: BoundStatement,
        bind_data: BindData,
    ) -> Result<HybridPlanResponse> {
        // Extend context with what we need in the query.
        let mut extender = HybridContextExtender::new(&mut context, &self.registry);
        extender.attach_unknown_databases(&bind_data).await?;

        // Now resolve with the extended context.
        let tx = CatalogTx::new();
        let resolver = HybridResolver::new(&tx, &context);
        let bind_data = resolver.resolve_all_unbound(bind_data).await?;

        // TODO: Remove session var requirement.
        let vars = SessionVars::new_local();

        let (mut logical, query_context) =
            PlanContext::new(&vars, &bind_data).plan_statement(stmt)?;

        let optimizer = Optimizer::new();
        logical.root = optimizer.optimize(logical.root)?;
        let schema = logical.schema()?;

        let query_id = Uuid::new_v4();

        let planner = IntermediatePipelinePlanner::new(IntermediateConfig::default(), query_id);
        let pipelines = planner.plan_pipelines(logical.root, query_context)?;

        self.pending_pipelines.insert(
            query_id,
            PendingPipelineState {
                context,
                group: pipelines.remote,
            },
        );

        Ok(HybridPlanResponse {
            query_id,
            pipelines: pipelines.local,
            schema,
        })
    }

    pub fn execute_pending(&self, query_id: Uuid) -> Result<()> {
        let (_, state) = self.pending_pipelines.remove(&query_id).ok_or_else(|| {
            RayexecError::new(format!("Missing pending pipeline for id: {query_id}"))
        })?;

        let mut planner = ExecutablePipelinePlanner::<R>::new(
            &state.context,
            ExecutionConfig {
                target_partitions: num_cpus::get(),
            },
            PlanLocationState::Server {
                stream_buffers: &self.buffers,
            },
        );

        // TODO: Spooky action, this needs to happen before the planning so that
        // planning can get the appropriate error sink when creating streams.
        let error_sink = self.buffers.create_error_sink(query_id)?;

        let pipelines = planner.plan_from_intermediate(state.group)?;
        let handle = self.executor.spawn_pipelines(pipelines, error_sink);

        self.executing_pipelines.insert(query_id, handle);

        Ok(())
    }

    pub fn push_batch_for_stream(&self, stream_id: StreamId, batch: Batch) -> Result<()> {
        self.buffers.push_batch_for_stream(&stream_id, batch)
    }

    pub fn finalize_stream(&self, stream_id: StreamId) -> Result<()> {
        self.buffers.finalize_stream(&stream_id)
    }

    pub fn pull_batch_for_stream(&self, stream_id: StreamId) -> Result<PullStatus> {
        self.buffers.pull_batch_for_stream(&stream_id)
    }
}
