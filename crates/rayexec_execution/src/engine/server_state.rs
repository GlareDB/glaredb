use std::sync::Arc;

use dashmap::DashMap;
use crate::arrays::batch::Batch;
use crate::arrays::field::{Field, Schema};
use rayexec_error::{not_implemented, RayexecError, Result};
use uuid::Uuid;

use crate::config::execution::{ExecutablePlanConfig, IntermediatePlanConfig};
use crate::config::session::SessionConfig;
use crate::database::catalog::CatalogTx;
use crate::database::DatabaseContext;
use crate::datasource::DataSourceRegistry;
use crate::execution::executable::planner::{ExecutablePipelinePlanner, PlanLocationState};
use crate::execution::intermediate::pipeline::{
    IntermediateMaterializationGroup,
    IntermediatePipelineGroup,
    StreamId,
};
use crate::execution::intermediate::planner::IntermediatePipelinePlanner;
use crate::hybrid::buffer::ServerStreamBuffers;
use crate::hybrid::client::{HybridPlanResponse, PullStatus};
use crate::logical::binder::bind_statement::StatementBinder;
use crate::logical::operator::LogicalOperator;
use crate::logical::planner::plan_statement::StatementPlanner;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::resolve_hybrid::{HybridContextExtender, HybridResolver};
use crate::logical::resolver::ResolvedStatement;
use crate::optimizer::Optimizer;
use crate::runtime::handle::QueryHandle;
use crate::runtime::{PipelineExecutor, Runtime};

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
    runtime: R,
}

#[derive(Debug)]
struct PendingPipelineState {
    /// Context we used for intial planning, and what we'll be using for
    /// executable pipeline planning.
    context: DatabaseContext,
    /// The pipeline group we'll be turning into executables pipelines.
    group: IntermediatePipelineGroup,
    materializations: IntermediateMaterializationGroup,
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
            runtime,
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
        stmt: ResolvedStatement,
        bind_data: ResolveContext,
    ) -> Result<HybridPlanResponse> {
        // Extend context with what we need in the query.
        let mut extender = HybridContextExtender::new(&mut context, &self.registry);
        extender.attach_unknown_databases(&bind_data).await?;

        // Now resolve with the extended context.
        let tx = CatalogTx::new();
        let resolver = HybridResolver::new(&tx, &context);
        let resolve_context = resolver.resolve_remaining(bind_data).await?;

        // TODO: Remove session config requirement.
        let session_config = SessionConfig::new(&self.executor, &self.runtime);

        let binder = StatementBinder {
            session_config: &session_config,
            resolve_context: &resolve_context,
        };
        let (bound_stmt, mut bind_context) = binder.bind(stmt)?;
        let mut logical = StatementPlanner.plan(&mut bind_context, bound_stmt)?;

        let mut optimizer = Optimizer::new();
        logical = optimizer.optimize::<R::Instant>(&mut bind_context, logical)?;

        // If we're an explain, put a copy of the optimized plan on the
        // node.
        if let LogicalOperator::Explain(explain) = &mut logical {
            let child = explain
                .children
                .first()
                .ok_or_else(|| RayexecError::new("Missing explain child"))?;
            explain.node.logical_optimized = Some(Box::new(child.clone()));
        }

        let schema = Schema::new(
            bind_context
                .iter_tables_in_scope(bind_context.root_scope_ref())?
                .flat_map(|t| {
                    t.column_names
                        .iter()
                        .zip(&t.column_types)
                        .map(|(name, datatype)| Field::new(name, datatype.clone(), true))
                }),
        );

        let query_id = Uuid::new_v4();
        let planner = IntermediatePipelinePlanner::new(IntermediatePlanConfig::default(), query_id);

        let pipelines = planner.plan_pipelines(logical, bind_context)?;

        if !pipelines.materializations.is_empty() {
            not_implemented!("materializations with hybrid exec")
        }

        self.pending_pipelines.insert(
            query_id,
            PendingPipelineState {
                context,
                group: pipelines.remote,
                materializations: IntermediateMaterializationGroup::default(),
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
            ExecutablePlanConfig {
                partitions: num_cpus::get(),
            },
            PlanLocationState::Server {
                stream_buffers: &self.buffers,
            },
        );

        // TODO: Spooky action, this needs to happen before the planning so that
        // planning can get the appropriate error sink when creating streams.
        let error_sink = self.buffers.create_error_sink(query_id)?;

        let pipelines = planner.plan_from_intermediate(state.group, state.materializations)?;
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
