use std::collections::VecDeque;
use std::time::Duration;

use parking_lot::Mutex;
use uuid::Uuid;

use crate::execution::planner::OperatorId;

/// Retain the profiles for the last number of queries.
pub const RETAINED_PROFILE_COUNT: usize = 20;

#[derive(Debug, Default)]
pub struct ProfileCollector {
    profiles: Mutex<VecDeque<QueryProfile>>,
}

impl ProfileCollector {
    pub fn push_profile(&self, profile: QueryProfile) {
        let mut profiles = self.profiles.lock();
        while profiles.len() > RETAINED_PROFILE_COUNT {
            let _ = profiles.pop_back();
        }
        profiles.push_front(profile);
    }

    /// Get the nth profile.
    ///
    /// '0' is for the most recent query.
    pub fn get_profile(&self, n: usize) -> Option<QueryProfile> {
        let profiles = self.profiles.lock();
        profiles.get(n).cloned()
    }

    pub fn get_profile_by_id(&self, id: Uuid) -> Option<QueryProfile> {
        let profiles = self.profiles.lock();
        profiles.iter().find(|prof| prof.id == id).cloned()
    }
}

#[derive(Debug, Clone)]
pub struct QueryProfile {
    pub id: Uuid,
    pub plan: Option<PlanningProfile>,
    pub execution: Option<ExecutionProfile>,
}

/// Profile data for the optimizer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OptimizerProfile {
    /// Total time take for all optimizer rules.
    pub total: Duration,
    /// Time taken for each optimizer rule, keyed by the rule name.
    pub timings: Vec<(&'static str, Duration)>,
}

/// Profile data for the planning step in a query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlanningProfile {
    /// Time take to resolve all database items (tables, functions, etc) in the query.
    pub resolve_step: Option<Duration>,
    /// Time take to bind the query.
    pub bind_step: Option<Duration>,
    /// Time taken to plan the query.
    pub plan_logical_step: Option<Duration>,
    /// Optimizer profiling info.
    pub plan_optimize_step: Option<OptimizerProfile>,
    /// Time taken to create the physical plan from the logical plan.
    pub plan_physical_step: Option<Duration>,
    /// Time taken to create the final pipelines from the physical plan.
    pub plan_executable_step: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct OperatorProfile {
    /// Name of the operator.
    pub operator_name: &'static str,
    /// Identifier for the operator.
    pub operator_id: OperatorId,
    /// Total time spent execution this operator.
    pub execution_duration: Duration,
    /// Total number of rows pushing into this operator within this pipeline.
    pub rows_in: u64,
    /// Total number of rows pulled out of this operator within this pipeline.
    pub rows_out: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionPipelineProfile {
    /// Partition index of this partition pipeline.
    pub partition_idx: usize,
    /// Profiles for each operator in this pipeline.
    pub operator_profiles: Vec<OperatorProfile>,
}

#[derive(Debug, Clone)]
pub struct ExecutionProfile {
    /// Profiles for each partition pipeline in the query.
    pub partition_pipeline_profiles: Vec<PartitionPipelineProfile>,
}
