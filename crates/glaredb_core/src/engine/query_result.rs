use std::sync::Arc;

use futures::TryStreamExt;
use glaredb_error::{Result, ResultExt};
use tracing::error;
use uuid::Uuid;

use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::ConcurrentColumnCollection;
use crate::arrays::collection::equal;
use crate::arrays::field::ColumnSchema;
use crate::catalog::profile::{ProfileCollector, QueryProfile};
use crate::config::session::DEFAULT_BATCH_SIZE;
use crate::execution::operators::results::streaming::ResultStream;
use crate::runtime::pipeline::QueryHandle;
use crate::runtime::profile_buffer::ProfileBuffer;

#[derive(Debug)]
pub struct QueryResult {
    pub query_id: Uuid,
    pub output_schema: ColumnSchema,
    pub output: Output,
}

// TODO: Cleanup. I got confused with profile vs profiles.
#[derive(Debug)]
pub struct StreamOutput {
    profile: Option<QueryProfile>,
    profiles: Arc<ProfileCollector>,
    handle: Arc<dyn QueryHandle>,
    stream: ResultStream,
}

impl StreamOutput {
    pub fn new(
        stream: ResultStream,
        profile: QueryProfile,
        handle: Arc<dyn QueryHandle>,
        profiles: Arc<ProfileCollector>,
    ) -> Self {
        StreamOutput {
            profile: Some(profile),
            profiles,
            handle,
            stream,
        }
    }

    pub async fn collect(&mut self) -> Result<Vec<Batch>> {
        let stream = &mut self.stream;
        let batches = stream.try_collect().await?;

        if let Some(mut profile) = self.profile.take() {
            match self.handle.generate_final_execution_profile() {
                Ok(execution) => {
                    profile.execution = Some(execution);
                }
                Err(e) => {
                    // Don't fail the query because we couldn't generate the
                    // final profile, just log.
                    error!(%e, "failed to generate final execution profile");
                }
            }

            self.profiles.push_profile(profile);
        }

        Ok(batches)
    }
}

/// Verifies that the output of the 'left' query matches the output of the
/// 'right' query by collecting rows from both sides, and comparing the results.
// TODO: This should be a bit more sophisticated by incorporating a 'TeeStream'
// of sorts where we sink into the collection at the same time as we produce the
// batches.
//
// This would be useful for "result caching" scenarios as well.
//
// This is pretty wasteful/inflexible as-is. But I'm fine with it since I just
// need if for testing.
#[derive(Debug)]
pub struct VerifyStreamOutput {
    pub(crate) left_stream: ResultStream,
    pub(crate) left_collection: Arc<ConcurrentColumnCollection>,

    pub(crate) right_profile: Option<QueryProfile>,
    pub(crate) right_profiles: Arc<ProfileCollector>,
    pub(crate) right_stream: ResultStream,
    pub(crate) right_collection: Arc<ConcurrentColumnCollection>,

    /// The combined left and right handles.
    pub(crate) handle: Arc<VerifyQueryHandle>,
}

impl VerifyStreamOutput {
    pub async fn collect(&mut self) -> Result<Vec<Batch>> {
        // Collect the left.
        let mut left_append = self.left_collection.init_append_state();
        while let Some(batch) = self.left_stream.try_next().await? {
            self.left_collection
                .append_batch(&mut left_append, &batch)?;
        }
        self.left_collection.flush(&mut left_append)?;

        // Collect the right.
        let right_stream = &mut self.right_stream;
        let batches: Vec<_> = right_stream.try_collect().await?;
        let mut right_append = self.right_collection.init_append_state();
        for batch in &batches {
            self.right_collection
                .append_batch(&mut right_append, batch)?;
        }
        self.right_collection.flush(&mut right_append)?;

        // Compare the results.
        equal::verify_collections_eq(
            &self.left_collection,
            &self.right_collection,
            DEFAULT_BATCH_SIZE,
        )
        .context("Query verification failed")?;

        // We're only going to concern ourselves with the timings from the right
        // plan (assumed to be the optimized plan).
        if let Some(mut profile) = self.right_profile.take() {
            match self.handle.right.generate_final_execution_profile() {
                Ok(execution) => {
                    profile.execution = Some(execution);
                }
                Err(e) => {
                    // Don't fail the query because we couldn't generate the
                    // final profile, just log.
                    error!(%e, "failed to generate final execution profile");
                }
            }

            self.right_profiles.push_profile(profile);
        }

        Ok(batches)
    }
}

#[derive(Debug)]
pub struct VerifyQueryHandle {
    pub(crate) left: Arc<dyn QueryHandle>,
    pub(crate) right: Arc<dyn QueryHandle>,
}

impl QueryHandle for VerifyQueryHandle {
    fn cancel(&self) {
        self.left.cancel();
        self.right.cancel();
    }

    fn get_profile_buffer(&self) -> &ProfileBuffer {
        // TODO: Is this fine?
        self.right.get_profile_buffer()
    }
}

#[derive(Debug)]
pub enum Output {
    Stream(StreamOutput),
    VerifyStream(VerifyStreamOutput),
}

impl Output {
    pub async fn collect(&mut self) -> Result<Vec<Batch>> {
        match self {
            Self::Stream(stream) => stream.collect().await,
            Self::VerifyStream(stream) => stream.collect().await,
        }
    }

    pub fn query_handle(&self) -> Arc<dyn QueryHandle> {
        match self {
            Self::Stream(stream) => stream.handle.clone(),
            Self::VerifyStream(stream) => stream.handle.clone() as _,
        }
    }
}
