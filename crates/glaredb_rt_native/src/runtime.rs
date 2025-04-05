use std::fmt::Debug;
use std::sync::Arc;

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::filesystem::dispatch::FileSystemDispatch;
use glaredb_core::runtime::pipeline::{ErrorSink, PipelineRuntime, QueryHandle};
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::{Result, ResultExt};
use tokio::runtime::Handle as TokioHandle;

use crate::filesystem::LocalFileSystem;
use crate::threaded::ThreadedScheduler;
use crate::time::NativeInstant;

/// Inner behavior of the execution runtime.
// TODO: Single-threaded scheduler to run our SLTs on to ensure no operators
// block without making progress. Would not be used for anything else.
pub trait Scheduler: Sync + Send + Debug + Sized + Clone {
    type Handle: QueryHandle;

    fn try_new(num_threads: usize) -> Result<Self>;

    fn num_threads(&self) -> usize;

    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Self::Handle;
}

#[derive(Debug, Clone)]
pub struct NativeExecutor<S: Scheduler>(S);

impl<S: Scheduler> NativeExecutor<S> {
    pub fn try_new() -> Result<Self> {
        let threads = num_cpus::get();
        Ok(NativeExecutor(S::try_new(threads)?))
    }

    pub fn try_new_with_num_threads(num_threads: usize) -> Result<Self> {
        Ok(NativeExecutor(S::try_new(num_threads)?))
    }
}

impl<S: Scheduler + 'static> PipelineRuntime for NativeExecutor<S> {
    fn default_partitions(&self) -> usize {
        self.0.num_threads()
    }

    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Arc<dyn QueryHandle> {
        let handle = self.0.spawn_pipelines(pipelines, errors);
        Arc::new(handle)
    }
}

/// Create a new tokio runtime configured for io.
///
/// This runtime should be passed to the native runtime so its able to execute
/// io operations on the tokio runtime.
pub fn new_tokio_runtime_for_io() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .thread_name("glaredb_tokio_io")
        .build()
        .context("Failed to build tokio runtime")
}

pub type ThreadedNativeExecutor = NativeExecutor<ThreadedScheduler>;

#[derive(Debug, Clone)]
pub struct NativeSystemRuntime {
    // TODO: If we want to have dynamically registered filesystems, it's likely
    // the dispatch would need to live on the session to get rid of arc.
    //
    // Which would be find, filesystems should internally be wrapped in an arc,
    // so the engine could have 'default' filesystems, then the session could
    // either be a super or subset of that.
    dispatch: Arc<FileSystemDispatch>,
}

impl NativeSystemRuntime {
    pub fn new(_handle: TokioHandle) -> Self {
        let mut dispatch = FileSystemDispatch::empty();

        // TODO: native fs, http stuff. Tokio wil be use there.
        dispatch.register_filesystem(LocalFileSystem {});

        NativeSystemRuntime {
            dispatch: Arc::new(dispatch),
        }
    }
}

impl SystemRuntime for NativeSystemRuntime {
    type Instant = NativeInstant;

    fn filesystem_dispatch(&self) -> &FileSystemDispatch {
        &self.dispatch
    }
}
