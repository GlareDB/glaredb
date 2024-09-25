use futures::future::BoxFuture;
use rayexec_error::Result;
use std::fmt::Debug;

use crate::execution::executable::profiler::ExecutionProfileData;

/// A handle to a running or recently completed query.
pub trait QueryHandle: Debug + Sync + Send {
    /// Cancel the query.
    ///
    /// This should be best effort, and makes no guarantee when the query will
    /// be canceled.
    fn cancel(&self);

    /// Generates profile data for the query.
    ///
    /// This is async as it's expected to fetch profiling data for pipelines
    /// executing on remote nodes.
    fn generate_execution_profile_data(&self) -> BoxFuture<'_, Result<ExecutionProfileData>>;
}
