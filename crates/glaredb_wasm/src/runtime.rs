use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::filesystem::dispatch::FileSystemDispatch;
use glaredb_core::runtime::pipeline::{ErrorSink, PipelineRuntime, QueryHandle};
use glaredb_core::runtime::profile_buffer::{ProfileBuffer, ProfileSink};
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::{DbError, Result};
use glaredb_http::filesystem::HttpFileSystem;
use glaredb_http::gcs::filesystem::GcsFileSystem;
use glaredb_http::s3::filesystem::S3FileSystem;
use parking_lot::Mutex;
use tracing::{debug, warn};
use wasm_bindgen_futures::spawn_local;

use crate::http::WasmHttpClient;
use crate::time::PerformanceInstant;

#[derive(Debug, Clone)]
pub struct WasmSystemRuntime {
    dispatch: Arc<FileSystemDispatch>,
}

impl WasmSystemRuntime {
    pub fn try_new() -> Result<Self> {
        let mut dispatch = FileSystemDispatch::empty();

        // Register http filesystem.
        let client = reqwest::Client::new();
        let client = WasmHttpClient::new(client);
        let http_fs = HttpFileSystem::new(client.clone());
        dispatch.register_filesystem(http_fs);

        // Register s3 filesystem.
        let s3_fs = S3FileSystem::new(client.clone(), "us-east-1");
        dispatch.register_filesystem(s3_fs);

        let gcs_fs = GcsFileSystem::new(client);
        dispatch.register_filesystem(gcs_fs);

        // TODO: When it works, aka we need web workers.
        // // Register origin filesystem.
        // dispatch.register_filesystem(OriginFileSystem {});

        // TODO: Shared memory fs

        Ok(WasmSystemRuntime {
            dispatch: Arc::new(dispatch),
        })
    }
}

impl SystemRuntime for WasmSystemRuntime {
    type Instant = PerformanceInstant;

    fn filesystem_dispatch(&self) -> &FileSystemDispatch {
        &self.dispatch
    }
}

/// Execution scheduler for wasm.
///
/// This implementation works on a single thread which each pipeline task being
/// spawned local to the thread (using js promises under the hood).
#[derive(Debug, Clone)]
pub struct WasmExecutor;

impl PipelineRuntime for WasmExecutor {
    fn default_partitions(&self) -> usize {
        1
    }

    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Arc<dyn QueryHandle> {
        debug!("spawning query graph on wasm runtime");

        let (profiles, profile_sinks) = ProfileBuffer::new(pipelines.len());

        let states: Vec<_> = pipelines
            .into_iter()
            .zip(profile_sinks)
            .map(|(pipeline, profile_sink)| {
                Arc::new(WasmTaskState {
                    profile_sink,
                    errors: errors.clone(),
                    pipeline: Mutex::new(WasmPipelineState {
                        pipeline,
                        query_canceled: false,
                        finished: false,
                    }),
                })
            })
            .collect();

        for state in &states {
            let state = state.clone();
            spawn_local(async move { state.execute() })
        }

        Arc::new(WasmQueryHandle { profiles, states })
    }
}

#[derive(Debug)]
pub(crate) struct WasmPipelineState {
    pub(crate) pipeline: ExecutablePartitionPipeline,
    pub(crate) query_canceled: bool,
    // TODO: See native runtime.
    pub(crate) finished: bool,
}

#[derive(Debug)]
pub(crate) struct WasmTaskState {
    profile_sink: ProfileSink,
    errors: Arc<dyn ErrorSink>,
    pipeline: Mutex<WasmPipelineState>,
}

impl Wake for WasmTaskState {
    fn wake(self: Arc<Self>) {
        spawn_local(async move { self.execute() })
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Arc::clone(self).wake()
    }
}

impl WasmTaskState {
    fn execute(self: Arc<Self>) {
        let mut pipeline_state = self.pipeline.lock();

        if pipeline_state.query_canceled {
            self.errors.set_error(DbError::new("Query canceled"));
            return;
        }

        if pipeline_state.finished {
            warn!("Attempted to execute task that's finished");
            return;
        }

        let waker: Waker = self.clone().into();
        let mut cx = Context::from_waker(&waker);

        match pipeline_state
            .pipeline
            .poll_execute::<PerformanceInstant>(&mut cx)
        {
            Poll::Ready(Ok(profile)) => {
                // Pushing through the pipeline was successful.
                self.profile_sink.put(profile);
            }
            Poll::Ready(Err(e)) => {
                self.errors.set_error(e);
            }
            Poll::Pending => {
                // Exit the loop. Waker was already stored in the pending
                // sink/source, we'll be woken back up when there's more
                // this operator chain can start executing.
            }
        }
    }
}

#[derive(Debug)]
pub struct WasmQueryHandle {
    profiles: ProfileBuffer,
    #[allow(unused)]
    states: Vec<Arc<WasmTaskState>>,
}

impl QueryHandle for WasmQueryHandle {
    fn cancel(&self) {
        // TODO
    }

    fn get_profile_buffer(&self) -> &ProfileBuffer {
        &self.profiles
    }
}
