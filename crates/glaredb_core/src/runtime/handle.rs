use std::fmt::Debug;

use glaredb_error::Result;

use super::profile_buffer::ProfileBuffer;
use crate::catalog::profile::ExecutionProfile;

/// A handle to a running or recently completed query.
pub trait QueryHandle: Debug + Sync + Send {
    /// Cancel the query.
    ///
    /// This should be best effort, and makes no guarantee when the query will
    /// be canceled.
    fn cancel(&self);

    /// Get a reference to the buffer holding the execution profiles.
    fn get_profile_buffer(&self) -> &ProfileBuffer;

    /// Generates the final execution profile for the query.
    ///
    /// This should only be called onces, and after executing the query to
    /// completion.
    // TODO: This may at some point require async if we need to fetch remote
    // profiles.
    fn generate_final_execution_profile(&self) -> Result<ExecutionProfile> {
        let buffer = self.get_profile_buffer();

        // TODO: We shouldn't filter out None profiles.
        let profiles = buffer.take_profiles()?.into_iter().flatten().collect();

        Ok(ExecutionProfile {
            partition_pipeline_profiles: profiles,
        })
    }
}
