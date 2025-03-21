use std::sync::Arc;

use futures::TryStreamExt;
use glaredb_error::Result;
use tracing::error;

use crate::arrays::batch::Batch;
use crate::arrays::field::ColumnSchema;
use crate::catalog::profile::{ProfileCollector, QueryProfile};
use crate::execution::operators::results::streaming::ResultStream;
use crate::runtime::handle::QueryHandle;

#[derive(Debug)]
pub struct QueryResult {
    pub output_schema: ColumnSchema,
    pub output: Output,
}

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

#[derive(Debug)]
pub enum Output {
    Stream(StreamOutput),
}

impl Output {
    pub async fn collect(&mut self) -> Result<Vec<Batch>> {
        match self {
            Self::Stream(stream) => stream.collect().await,
        }
    }

    pub fn query_handle(&self) -> &Arc<dyn QueryHandle> {
        match self {
            Self::Stream(stream) => &stream.handle,
        }
    }
}
