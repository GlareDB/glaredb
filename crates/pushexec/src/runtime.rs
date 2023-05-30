use crate::errors::{PushExecError, Result};
use core_affinity::CoreId;
use rayon::{ThreadPool as RayonThreadPool, ThreadPoolBuilder as RayonBuilder};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::{Builder as TokioBuilder, Runtime as TokioRuntime};
use tracing::{error, info};

/// Thread pools for sync and async tasks.
#[derive(Clone)]
pub struct ExecRuntime {
    pub tokio_rt: Arc<TokioRuntime>,
    pub rayon_rt: Arc<RayonThreadPool>,
}

/// Configure how the runtimes should be constructed.
#[derive(Debug, Clone)]
enum ThreadsConfig {
    /// Use the specific core ids for tokio worker threads.
    CoreIds(Vec<CoreId>),
    /// Use some number of worker threads. Threads will not have a core affinity
    /// set.
    FixedNumber(usize),
}

impl ThreadsConfig {
    fn num_threads(&self) -> usize {
        match self {
            ThreadsConfig::CoreIds(v) => v.len(),
            Self::FixedNumber(n) => *n,
        }
    }
}

impl ExecRuntime {
    /// Create a new runtime containing both a tokio runtime and a rayon thread
    /// pool.
    ///
    /// The size of the rayon thread pool will match the number of cpus on the
    /// system. The tokio runtime will only have half the number of worker
    /// threads. These numbers are based on intuition only. More work will need
    /// to be done to figure out what good default numbers are.
    ///
    /// This will also attempt to set core affinity for both.
    ///
    /// Note that this runtime should _not_ be use for metastore or pgsrv. Both
    /// of those only use tokio, and the tokio runtime created here would not be
    /// suitable for those services.
    pub fn new() -> Result<ExecRuntime> {
        let core_ids = core_affinity::get_core_ids();

        let tokio_conf = match core_ids.clone() {
            Some(ids) => {
                let len = ids.len() / 2;
                let ids = ids.into_iter().take(len).collect();
                ThreadsConfig::CoreIds(ids)
            }
            None => ThreadsConfig::FixedNumber(num_cpus::get() / 2),
        };
        let rayon_conf = match core_ids {
            Some(ids) => ThreadsConfig::CoreIds(ids),
            None => ThreadsConfig::FixedNumber(num_cpus::get()),
        };

        info!(?tokio_conf, ?rayon_conf, "building exec runtime");

        Ok(ExecRuntime {
            tokio_rt: Arc::new(Self::build_tokio(tokio_conf)?),
            rayon_rt: Arc::new(Self::build_rayon(rayon_conf)?),
        })
    }

    fn build_tokio(conf: ThreadsConfig) -> Result<TokioRuntime> {
        let rt = TokioBuilder::new_multi_thread()
            .worker_threads(conf.num_threads())
            .on_thread_start(move || {
                static CORE_IDX: AtomicUsize = AtomicUsize::new(0);
                let idx = CORE_IDX.fetch_add(1, Ordering::SeqCst);

                if let ThreadsConfig::CoreIds(v) = &conf {
                    let core_id = v[idx % conf.num_threads()];
                    core_affinity::set_for_current(core_id);
                }
            })
            .thread_name_fn(move || {
                static THREAD_ID: AtomicUsize = AtomicUsize::new(0);
                let id = THREAD_ID.fetch_add(1, Ordering::SeqCst);
                format!("tokio-worker-thread-{}", id)
            })
            .enable_all()
            .build()
            .map_err(|e| PushExecError::BuildRuntime {
                context: "tokio runtime".to_string(),
                error: Box::new(e),
            })?;

        Ok(rt)
    }

    fn build_rayon(conf: ThreadsConfig) -> Result<RayonThreadPool> {
        let rt = RayonBuilder::new()
            .num_threads(conf.num_threads())
            .start_handler(move |idx| {
                if let ThreadsConfig::CoreIds(v) = &conf {
                    let core_id = v[idx];
                    core_affinity::set_for_current(core_id);
                }
            })
            .panic_handler(|p| {
                let panic = format_worker_panic(p);
                error!(%panic, "worker panicked");
            })
            .thread_name(move |idx| format!("rayon-worker-thread-{}", idx))
            .build()
            .map_err(|e| PushExecError::BuildRuntime {
                context: "rayon runtime".to_string(),
                error: Box::new(e),
            })?;

        Ok(rt)
    }
}

/// Formats a panic message for a rayon worker.
fn format_worker_panic(panic: Box<dyn std::any::Any + Send>) -> String {
    let maybe_idx = rayon::current_thread_index();
    let worker: &dyn std::fmt::Display = match &maybe_idx {
        Some(idx) => idx,
        None => &"UNKNOWN",
    };

    let message = if let Some(msg) = panic.downcast_ref::<&str>() {
        *msg
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.as_str()
    } else {
        "UNKNOWN"
    };

    format!("worker {} panicked with: {}", worker, message)
}
