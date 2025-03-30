use std::fmt::Debug;
use std::sync::Arc;

use glaredb_error::{DbError, Result};

use super::profile_buffer::ProfileBuffer;
use crate::catalog::profile::ExecutionProfile;
use crate::execution::partition_pipeline::ExecutablePartitionPipeline;

/// How pipelines get executed on a single node.
///
/// This is trait only concerns itself with the low-level execution of
/// pipelines. Higher level concepts like hybrid and distributed execution build
/// on top of these traits.
///
/// This will likely only ever have two implementations; one for when we're
/// executing "natively" (running pipelines on a thread pool), and one for wasm.
pub trait PipelineRuntime: Debug + Sync + Send + Clone {
    /// Number of partitions to default to when executing.
    fn default_partitions(&self) -> usize;

    /// Spawn execution of multiple pipelines for a query.
    ///
    /// A query handle will be returned allowing for canceling and dumping a
    /// query.
    ///
    /// When execution encounters an unrecoverable error, the error will be
    /// written to the provided error sink. Recoverable errors should be handled
    /// internally.
    ///
    /// This must not block.
    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Arc<dyn QueryHandle>;
}

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

/// Where to put errors that happen during execution.
///
/// This will get passed to each pipeline task, and if task encounters an error,
/// it'll push the error here.
pub trait ErrorSink: Debug + Sync + Send {
    /// Push an error.
    fn set_error(&self, error: DbError);
}
