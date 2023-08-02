pub mod storage;

use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tracing::{debug, error, info};

use crate::errors::{ExecError, Result};

#[derive(Debug)]
pub struct JobRunnerOpts {
    pub health_check_interval: Duration,
}

impl Default for JobRunnerOpts {
    fn default() -> Self {
        Self {
            // Health check every 2 minutes.
            health_check_interval: Duration::from_secs(2 * 60),
        }
    }
}

/// Collection of background jobs.
#[derive(Debug, Clone)]
pub struct JobRunner {
    sender: mpsc::UnboundedSender<RequestMessage>,
    listen: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Debug)]
enum RequestMessage {
    NewJob(Arc<dyn BgJob>),
    JobComplete(String, Result<()>),
    HealthCheck,
    Close,
}

impl JobRunner {
    /// Create a new collection of background jobs.
    pub fn new(opts: JobRunnerOpts) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        // Sends a "health check" mesage to the channel.
        let sender_clone = sender.clone();
        tokio::spawn(async move {
            let sender = sender_clone;

            let mut health_check = tokio::time::interval(opts.health_check_interval);
            loop {
                let _ = health_check.tick().await;
                if sender.send(RequestMessage::HealthCheck).is_err() {
                    debug!("exiting background jobs health checker");
                    return;
                }
            }
        });

        // Spawn the listener for receiving messages.
        let sender_clone = sender.clone();
        let listen = tokio::spawn(async move { Self::listen(sender_clone, receiver).await });
        let listen = Arc::new(Mutex::new(Some(listen)));
        Self { sender, listen }
    }

    /// Listens on the channel for new messages.
    async fn listen(
        sender: mpsc::UnboundedSender<RequestMessage>,
        mut receiver: mpsc::UnboundedReceiver<RequestMessage>,
    ) {
        struct JobHandle {
            join: JoinHandle<()>,
            sleep: JoinHandle<()>,
        }

        let mut jobs: HashMap<String, JobHandle> = HashMap::new();
        while let Some(msg) = receiver.recv().await {
            debug!(?msg, "background job runner received a message");

            match msg {
                RequestMessage::NewJob(job) => {
                    let job_name = job.name();
                    info!(%job_name, "creating new background job");

                    if jobs.contains_key(&job_name) {
                        continue;
                    }

                    let (sleep_sender, sleep_receiver) = oneshot::channel();
                    let deadline = job.start_at();

                    let sleep_handle = tokio::spawn(async move {
                        // Sleep till the job is set to `start_at`.
                        tokio::time::sleep_until(deadline).await;
                        sleep_sender.send(()).unwrap();
                    });

                    let sender_clone = sender.clone();
                    let job_name_clone = job_name.clone();

                    let join_handle = tokio::spawn(async move {
                        let sender = sender_clone;
                        let job_name = job_name_clone;

                        // Wait for the time we need to sleep.
                        let _ = sleep_receiver.await;

                        let res = job.start().await;
                        if sender
                            .send(RequestMessage::JobComplete(job_name.clone(), res))
                            .is_err()
                        {
                            // The job is complete (after close is called).
                            info!(%job_name, "background job exited");
                        }
                    });

                    // Insert the job in the collection.
                    jobs.insert(
                        job_name,
                        JobHandle {
                            join: join_handle,
                            sleep: sleep_handle,
                        },
                    );
                }
                RequestMessage::JobComplete(job_name, res) => {
                    match res {
                        Ok(()) => info!(%job_name, "background job completed"),
                        // TODO: Once we have a persistent map of job_name ->
                        // options, we can set the job to retry on failure.
                        // Since further jobs are batched, this shouldn't be
                        // an issue.
                        Err(error) => error!(%job_name, %error, "background job exited with error"),
                    };
                    jobs.remove(&job_name);
                }
                RequestMessage::HealthCheck => {
                    jobs.retain(|_, handle| !handle.join.is_finished());
                }
                RequestMessage::Close => {
                    info!("close signal received, waiting for all background jobs to complete");

                    // Stop listening for more messages immediately.
                    receiver.close();

                    // Wait for all jobs to complete.
                    for handle in jobs.into_values() {
                        let JobHandle { join, sleep } = handle;

                        // Abort the sleeping period so we can run the
                        // pending jobs immediately.
                        if !sleep.is_finished() {
                            sleep.abort();
                        }

                        // There's a possible panic but since we're
                        // closing, we'd want to ignore this.
                        let _ = join.await;
                    }

                    info!("all background jobs completed");

                    // TODO: Handle pending messages in the buffer elegantly.
                    // Though rare, but it may happen that there are `NewJob`
                    // messages in the buffer. Once we have a persistent map,
                    // drain all the jobs back into the map as incomplete.
                    break;
                }
            }
        }
    }

    /// Sends the close message to the listener.
    pub async fn close(&self) -> Result<()> {
        // Send the "close" message.
        self.sender
            .send(RequestMessage::Close)
            .map_err(|e| ExecError::ChannelSendError(Box::new(e)))?;

        // Wait for listener to exit.
        let handle = {
            let mut guard = self.listen.lock();
            match guard.take() {
                Some(handle) => handle,
                None => return Ok(()),
            }
        };
        let _ = handle.await;

        Ok(())
    }

    /// Add a new background job.
    pub fn add(&self, job: Arc<dyn BgJob>) -> Result<()> {
        match self.sender.send(RequestMessage::NewJob(job)) {
            Ok(_) => Ok(()),
            Err(e) => match &e {
                mpsc::error::SendError(RequestMessage::NewJob(job)) => {
                    // We'll want to know when we fail to send as we may end up
                    // leaking resources somewhere that will need manual
                    // cleanup.
                    error!(name = %job.name(), "unexpected error from send channel");
                    Err(ExecError::ChannelSendError(Box::new(e)))
                }
                mpsc::error::SendError(msg) => {
                    // Technically unreachable since we should be getting back what
                    // we just sent.
                    error!(?msg, "unexpected msg from send error");
                    Err(ExecError::ChannelSendError(Box::new(e)))
                }
            },
        }
    }

    /// Add many background jobs.
    pub fn add_many(&self, jobs: impl IntoIterator<Item = Arc<dyn BgJob>>) -> Result<()> {
        for job in jobs {
            self.add(job)?;
        }
        Ok(())
    }
}
#[async_trait]
pub trait BgJob: Debug + Send + Sync {
    /// Name of the job.
    ///
    /// This should be a unique name but similar jobs that need to batched
    /// together should have the same name. For example, dropping table `xyz`
    /// should have the same name anywhere even if other options vary a bit.
    fn name(&self) -> String;

    /// The job should start at this instant (and not before this).
    fn start_at(&self) -> Instant;

    /// Main function that runs the job.
    async fn start(&self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    #[derive(Debug)]
    struct BackgroundJobTest {
        name: String,
        start_at: Instant,
        panic: bool,
        sender: mpsc::UnboundedSender<Instant>,
    }

    impl BackgroundJobTest {
        fn new(
            suffix: Option<&'static str>,
            batched: bool,
            panic: bool,
            sender: mpsc::UnboundedSender<Instant>,
        ) -> Arc<Self> {
            let mut name = "debug".to_string();
            if let Some(suffix) = suffix {
                write!(&mut name, "_{suffix}").unwrap();
            }

            let start_at = if batched {
                Instant::now() + Duration::from_secs(1)
            } else {
                Instant::now()
            };

            Arc::new(Self {
                name,
                start_at,
                panic,
                sender,
            })
        }
    }

    #[async_trait]
    impl BgJob for BackgroundJobTest {
        fn name(&self) -> String {
            self.name.clone()
        }

        fn start_at(&self) -> Instant {
            self.start_at
        }

        async fn start(&self) -> Result<()> {
            if self.panic {
                panic!("expected panic");
            }

            self.sender.send(Instant::now()).unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_background_jobs() {
        let jobs = JobRunner::new(JobRunnerOpts {
            health_check_interval: Duration::from_secs(2),
        });

        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Start an instantaneous job.
        let start = Instant::now();
        jobs.add(BackgroundJobTest::new(None, false, false, sender.clone()))
            .unwrap();

        let end = receiver.recv().await.unwrap();
        assert!(end - start < Duration::from_millis(500));

        // Start a batched job (that's executed after 2 seconds).
        let start = Instant::now();
        jobs.add(BackgroundJobTest::new(
            Some("batched"),
            true,
            false,
            sender.clone(),
        ))
        .unwrap();

        // Try to add a new job with the same name immediately.
        jobs.add(BackgroundJobTest::new(
            Some("batched"),
            true,
            false,
            sender.clone(),
        ))
        .unwrap();

        let end = receiver.recv().await.unwrap();
        assert!(end - start > Duration::from_secs(1));

        // Can only receive once here.
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

        // Health check by adding a job that panics and another that doesn't.
        jobs.add(BackgroundJobTest::new(
            Some("might_panic"),
            false,
            true,
            sender.clone(),
        ))
        .unwrap();

        jobs.add(BackgroundJobTest::new(
            Some("batched"),
            true, // batched so that health checker runs once
            false,
            sender.clone(),
        ))
        .unwrap();

        // Should be able to receive only once (for the valid job).
        let _ = receiver.recv().await.unwrap();
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

        // But we can add another job with the same name that won't panic now.
        jobs.add(BackgroundJobTest::new(
            Some("might_panic"),
            false,
            false,
            sender.clone(),
        ))
        .unwrap();

        let _ = receiver.recv().await.unwrap();
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

        // Adding a new batched job should work now.
        let start = Instant::now();
        jobs.add(BackgroundJobTest::new(
            Some("batched"),
            true,
            false,
            sender.clone(),
        ))
        .unwrap();

        // Close the job runner (this should wait for all existing jobs to
        // complete).

        // Add another job (to test close).
        jobs.add(BackgroundJobTest::new(
            Some("another_batched"),
            true,
            false,
            sender.clone(),
        ))
        .unwrap();

        jobs.close().await.unwrap();

        // Sleep should cancel here and the operation should run very quickly.
        let end1 = receiver.recv().await.unwrap();
        let end2 = receiver.recv().await.unwrap();
        let end = if end1 > end2 { end1 } else { end2 };
        assert!(end - start < Duration::from_millis(500));

        // We shouldn't be able to add jobs now.
        jobs.add(BackgroundJobTest::new(None, false, false, sender.clone()))
            .unwrap_err();
    }
}
