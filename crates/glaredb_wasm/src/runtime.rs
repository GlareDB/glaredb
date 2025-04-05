use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::filesystem::dispatch::FileSystemDispatch;
use glaredb_core::runtime::pipeline::{ErrorSink, PipelineRuntime, QueryHandle};
use glaredb_core::runtime::profile_buffer::{ProfileBuffer, ProfileSink};
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::Result;
use parking_lot::Mutex;
use tracing::debug;
use wasm_bindgen_futures::spawn_local;

use crate::time::PerformanceInstant;

#[derive(Debug, Clone)]
pub struct WasmSystemRuntime {
    dispatch: Arc<FileSystemDispatch>,
}

impl WasmSystemRuntime {
    pub fn try_new() -> Result<Self> {
        // TODO: Shared memory fs
        Ok(WasmSystemRuntime {
            dispatch: Arc::new(FileSystemDispatch::empty()),
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
            .map(|(pipeline, profile_sink)| WasmPartitionPipelineTask {
                state: Arc::new(WasmTaskState {
                    profile_sink,
                    errors: errors.clone(),
                    pipeline: Mutex::new(pipeline),
                }),
            })
            .collect();

        // TODO: Put references into query handle to allow canceling.

        for state in &states {
            let state = state.clone();
            spawn_local(async move { state.execute() })
        }

        Arc::new(WasmQueryHandle { profiles, states })
    }
}

#[derive(Debug)]
pub(crate) struct WasmTaskState {
    profile_sink: ProfileSink,
    errors: Arc<dyn ErrorSink>,
    pipeline: Mutex<ExecutablePartitionPipeline>,
}

#[derive(Debug, Clone)]
pub(crate) struct WasmPartitionPipelineTask {
    state: Arc<WasmTaskState>,
}

impl WasmPartitionPipelineTask {
    fn execute(&self) {
        let waker: Waker = Arc::new(WasmWaker {
            state: self.state.clone(),
        })
        .into();
        let mut cx = Context::from_waker(&waker);

        let mut pipeline = self.state.pipeline.lock();
        match pipeline.poll_execute::<PerformanceInstant>(&mut cx) {
            Poll::Ready(Ok(profile)) => {
                // Pushing through the pipeline was successful.
                self.state.profile_sink.put(profile);
            }
            Poll::Ready(Err(e)) => {
                self.state.errors.set_error(e);
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
    states: Vec<WasmPartitionPipelineTask>,
}

impl QueryHandle for WasmQueryHandle {
    fn cancel(&self) {
        // TODO
    }

    fn get_profile_buffer(&self) -> &ProfileBuffer {
        &self.profiles
    }
}

#[derive(Debug)]
struct WasmWaker {
    state: Arc<WasmTaskState>,
}

impl Wake for WasmWaker {
    fn wake_by_ref(self: &Arc<Self>) {
        self.clone().wake()
    }

    fn wake(self: Arc<Self>) {
        let task = WasmPartitionPipelineTask {
            state: self.state.clone(),
        };
        spawn_local(async move { task.execute() })
    }
}
