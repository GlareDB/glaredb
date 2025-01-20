use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use rayexec_io::http::HttpClient;

use super::pipeline::{ExecutablePipeline, PipelineId};
use crate::config::execution::ExecutablePlanConfig;
use crate::database::DatabaseContext;
use crate::engine::result::ResultSink;
use crate::execution::intermediate::pipeline::{
    IntermediateMaterializationGroup,
    IntermediateOperator,
    IntermediatePipelineGroup,
    IntermediatePipelineId,
    PipelineSink,
    PipelineSource,
};
use crate::execution::operators::materialize::MaterializeOperation;
use crate::execution::operators::round_robin::PhysicalRoundRobinRepartition;
use crate::execution::operators::sink::operation::SinkOperation;
use crate::execution::operators::sink::PhysicalSink;
use crate::execution::operators::source::operation::SourceOperation;
use crate::execution::operators::source::PhysicalSource;
use crate::execution::operators::{
    ExecutableOperator,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PhysicalOperator,
};
use crate::hybrid::buffer::ServerStreamBuffers;
use crate::hybrid::client::HybridClient;
use crate::hybrid::stream::{ClientToServerStream, ServerToClientStream};
use crate::logical::binder::bind_context::MaterializationRef;
use crate::runtime::Runtime;

/// Used for ensuring every pipeline in a query has a unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineIdGen {
    gen: PipelineId,
}

impl PipelineIdGen {
    fn next(&mut self) -> PipelineId {
        let id = self.gen;
        self.gen.0 += 1;
        id
    }
}

#[derive(Debug)]
pub enum PlanLocationState<'a, C: HttpClient> {
    /// State when planning on the server.
    Server {
        /// Stream buffers used for buffering incoming and outgoing batches for
        /// distributed execution.
        stream_buffers: &'a ServerStreamBuffers,
    },
    /// State when planning on the client side.
    Client {
        /// Output sink for a query.
        ///
        /// Should only be used once per query. The option helps us enforce that
        /// (and also allows us to avoid needing to wrap in an Arc).
        output_sink: Option<ResultSink>,
        /// Optional hybrid client if we're executing in hybrid mode.
        ///
        /// When providing, appropriate query sinks and sources will be inserted
        /// to the plan which will work to move batches between the client an
        /// server.
        hybrid_client: Option<&'a Arc<HybridClient<C>>>,
    },
}

#[derive(Debug)]
pub struct ExecutablePipelinePlanner<'a, R: Runtime> {
    context: &'a DatabaseContext,
    config: ExecutablePlanConfig,
    id_gen: PipelineIdGen,
    /// Location specific state used during planning.
    loc_state: PlanLocationState<'a, R::HttpClient>,
}

impl<'a, R: Runtime> ExecutablePipelinePlanner<'a, R> {
    pub fn new(
        context: &'a DatabaseContext,
        config: ExecutablePlanConfig,
        loc_state: PlanLocationState<'a, R::HttpClient>,
    ) -> Self {
        ExecutablePipelinePlanner {
            context,
            config,
            id_gen: PipelineIdGen { gen: PipelineId(0) },
            loc_state,
        }
    }

    pub fn plan_pipelines(
        &mut self,
        group: IntermediatePipelineGroup,
        materializations: IntermediateMaterializationGroup,
    ) -> Result<Vec<ExecutablePipeline>> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct InProgressPipeline {}
