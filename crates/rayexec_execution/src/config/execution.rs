use rayexec_error::{RayexecError, Result};

/// Configuration for intermediate pipeline planning.
#[derive(Debug, Clone)]
pub struct OperatorPlanConfig {
    /// If we should allow nested loop join.
    pub allow_nested_loop_join: bool,
}

impl Default for OperatorPlanConfig {
    fn default() -> Self {
        OperatorPlanConfig {
            allow_nested_loop_join: true,
        }
    }
}

impl OperatorPlanConfig {
    pub fn check_nested_loop_join_allowed(&self) -> Result<()> {
        if !self.allow_nested_loop_join {
            return Err(RayexecError::new("Nested loop join not allowed"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExecutablePlanConfig {
    /// Target number of partitions in executable pipelines.
    ///
    /// Partitionining determines parallelism for a single pipeline.
    pub partitions: usize,
    /// Target batch size.
    pub batch_size: usize,
}
