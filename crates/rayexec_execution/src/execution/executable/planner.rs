use rayexec_error::{RayexecError, Result};

use super::pipeline::{ExecutablePipeline, PipelineId};
use crate::config::execution::ExecutablePlanConfig;
use crate::database::DatabaseContext;

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
pub struct ExecutablePipelinePlanner<'a> {
    context: &'a DatabaseContext,
    config: ExecutablePlanConfig,
    id_gen: PipelineIdGen,
}

impl<'a> ExecutablePipelinePlanner<'a> {
    pub fn new(context: &'a DatabaseContext, config: ExecutablePlanConfig) -> Self {
        ExecutablePipelinePlanner {
            context,
            config,
            id_gen: PipelineIdGen { gen: PipelineId(0) },
        }
    }

    pub fn plan_pipelines(&mut self) -> Result<Vec<ExecutablePipeline>> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct InProgressPipeline {}
