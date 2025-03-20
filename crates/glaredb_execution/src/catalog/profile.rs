use std::collections::VecDeque;
use std::time::Duration;

use parking_lot::Mutex;
use uuid::Uuid;

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
