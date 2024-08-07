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
    logical::sql::{
        binder::{bind_data::BindData, hybrid::HybridResolver, BoundStatement},
        planner::PlanContext,
    },
    optimizer::Optimizer,
    runtime::{NopErrorSink, PipelineExecutor, QueryHandle, Runtime},
};
use std::sync::Arc;

/// A "server" session for doing remote planning and remote execution.
///
/// Keeps no state and very cheap to create. Essentially just encapsulates logic
/// for what should happen on the remote side for hybrid/distributed execution.
#[derive(Debug)]
pub struct ServerSession<P: PipelineExecutor, R: Runtime> {
    /// Context this session has access to.
    context: DatabaseContext,

    /// Registered data source implementations.
    _registry: Arc<DataSourceRegistry>,

    /// Hybrid execution streams.
    buffers: ServerStreamBuffers,

    pending_pipelines: DashMap<Uuid, IntermediatePipelineGroup>,
    executing_pipelines: DashMap<Uuid, Box<dyn QueryHandle>>,

    executor: P,
    _runtime: R,
}

impl<P, R> ServerSession<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    pub fn new(
        context: DatabaseContext,
        executor: P,
        runtime: R,
        registry: Arc<DataSourceRegistry>,
    ) -> Self {
        ServerSession {
            context,
            _registry: registry,
            buffers: ServerStreamBuffers::default(),
            pending_pipelines: DashMap::new(),
            executing_pipelines: DashMap::new(),
            executor,
            _runtime: runtime,
        }
    }

    // TODO: The only "unique" thing about this session is the context. This
    // session should be renamed to something else as it's probably easiest to
    // just have one of these per process, and instead the context should be
    // passed in as an arg where needed.
    pub fn context(&self) -> &DatabaseContext {
        &self.context
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
        stmt: BoundStatement,
        bind_data: BindData,
    ) -> Result<HybridPlanResponse> {
        let tx = CatalogTx::new();
        let resolver = HybridResolver::new(&tx, &self.context);
        let bind_data = resolver.resolve_all_unbound(bind_data).await?;

        // TODO: Remove session var requirement.
        let vars = SessionVars::new_local();

        let (mut logical, context) = PlanContext::new(&vars, &bind_data).plan_statement(stmt)?;

        let optimizer = Optimizer::new();
        logical.root = optimizer.optimize(logical.root)?;
        let schema = logical.schema()?;

        let planner = IntermediatePipelinePlanner::new(IntermediateConfig::default());
        let pipelines = planner.plan_pipelines(logical.root, context)?;

        let query_id = Uuid::new_v4();

        self.pending_pipelines.insert(query_id, pipelines.remote);

        Ok(HybridPlanResponse {
            query_id,
            pipelines: pipelines.local,
            schema,
        })
    }

    pub fn execute_pending(&self, query_id: Uuid) -> Result<()> {
        let (_, group) = self.pending_pipelines.remove(&query_id).ok_or_else(|| {
            RayexecError::new(format!("Missing pending pipeline for id: {query_id}"))
        })?;

        let mut planner = ExecutablePipelinePlanner::<R>::new(
            &self.context,
            ExecutionConfig {
                target_partitions: num_cpus::get(),
            },
            PlanLocationState::Server {
                stream_buffers: &self.buffers,
            },
        );

        let pipelines = planner.plan_from_intermediate(group)?;
        let handle = self
            .executor
            .spawn_pipelines(pipelines, Arc::new(NopErrorSink));

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
