//! Background jobs.
pub mod errors;
pub mod storage;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Interval;
use tracing::{debug, error};

const DEBUG_DURATION: Duration = Duration::from_secs(60);

pub trait BackgroundJob: Send + fmt::Debug {
    /// Return the interval that this job should run on.
    ///
    /// This should return an interval with the appropriate skipped behavior
    /// necessary for the job.
    fn interval(&self) -> Interval;

    /// Return the future for completing the job. The future will be sent to
    /// some background thread for execution.
    fn job_fut(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// A simple job that prints a debug log at some interval. Useful to check that
/// the background worker is running.
#[derive(Debug)]
pub struct DebugJob;

impl BackgroundJob for DebugJob {
    fn interval(&self) -> Interval {
        tokio::time::interval(DEBUG_DURATION)
    }

    fn job_fut(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {
            debug!("debug interval hit");
        })
    }
}

/// Run all background jobs on periodic intervals.
#[derive(Debug)]
pub struct BackgroundWorker {
    jobs: Vec<Box<dyn BackgroundJob>>,
    // NOTE: Only currently used to avoid dropping tokio task handles.
    shutdown: oneshot::Receiver<()>,
}

impl BackgroundWorker {
    pub fn new(
        jobs: impl IntoIterator<Item = Box<dyn BackgroundJob>>,
        shutdown: oneshot::Receiver<()>,
    ) -> BackgroundWorker {
        let jobs = jobs.into_iter().collect();
        BackgroundWorker { jobs, shutdown }
    }

    /// Begin the background worker.
    ///
    /// Note that this handles all errors internally. Errors should not stop the
    /// worker from continuing to process jobs.
    pub async fn begin(self) {
        debug!(jobs = ?self.jobs, "starting background worker");

        // Spin up a thread for each job.
        for job in self.jobs.into_iter() {
            let mut interval = job.interval();
            let _handle = tokio::spawn(async move {
                loop {
                    interval.tick().await;
                    let job_fut = job.job_fut();
                    job_fut.await;
                }
            });
        }

        if let Err(e) = self.shutdown.await {
            error!(%e, "failed to await background worker shutdown channel");
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug, Default, Clone)]
    struct DummyJob {
        hits: Arc<AtomicUsize>,
    }

    impl BackgroundJob for DummyJob {
        fn interval(&self) -> Interval {
            tokio::time::interval(Duration::from_millis(100))
        }

        fn job_fut(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                self.hits.fetch_add(1, Ordering::Relaxed);
            })
        }
    }

    #[tokio::test]
    async fn independent_loops() {
        let dummy1 = DummyJob::default();
        let dummy2 = DummyJob::default();

        let (tx, rx) = oneshot::channel();
        // Note we clone to retain a reference to atomic counters.
        let worker = BackgroundWorker::new(
            [
                Box::new(dummy1.clone()) as Box<dyn BackgroundJob>,
                Box::new(dummy2.clone()) as Box<dyn BackgroundJob>,
            ],
            rx,
        );

        tokio::spawn(worker.begin());

        // Ensure at least two iterations have passed for the dummy jobs.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Shutdown
        tx.send(()).unwrap();

        // Check that each dummy job was called at least twice.
        let hits1 = dummy1.hits.load(Ordering::Relaxed);
        assert!(hits1 >= 2);
        let hits2 = dummy2.hits.load(Ordering::Relaxed);
        assert!(hits2 >= 2);
    }
}
