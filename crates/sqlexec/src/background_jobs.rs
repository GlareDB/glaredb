// FIXME: Remove this once we have real jobs.
#![allow(dead_code)]

use std::{
    collections::HashMap,
    fmt::{Debug, Write},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tracing::{debug, error, info};

use crate::errors::{ExecError, Result};

/// Collection of background jobs.
pub struct JobRunner {
    sender: mpsc::UnboundedSender<RequestMessage>,
    listen: Arc<Mutex<Option<JoinHandle<()>>>>,
}

struct JobHandle {
    join: JoinHandle<()>,
    sleep: JoinHandle<()>,
}

#[derive(Debug)]
enum RequestMessage {
    NewJob(Box<dyn BgJob>),
    JobComplete(String, Result<()>),
    HealthCheck,
    Close,
}

impl JobRunner {
    /// Create a new collection of background jobs.
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
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
        // Sends a "health check" mesage to the channel every two minutes.
        let sender_clone = sender.clone();
        tokio::spawn(async move {
            let sender = sender_clone;

            let tick_interval = if cfg!(test) {
                Duration::from_secs(2)
            } else {
                Duration::from_secs(2 * 60)
            };

            let mut health_check = tokio::time::interval(tick_interval);
            loop {
                let _ = health_check.tick().await;
                sender.send(RequestMessage::HealthCheck).unwrap();
            }
        });

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
                        sender
                            .send(RequestMessage::JobComplete(job_name, res))
                            .unwrap();
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
    pub fn add(&self, options: BackgroundJobOptions) -> Result<()> {
        let job = match options {
            BackgroundJobOptions::Debug(debug_opts) => {
                Box::new(BackgroundJobDebug::new(debug_opts))
            }
        };

        self.sender
            .send(RequestMessage::NewJob(job))
            .map_err(|e| ExecError::ChannelSendError(Box::new(e)))?;

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

pub enum BackgroundJobOptions {
    Debug(BackgroundJobOptionsDebug),
}

pub struct BackgroundJobOptionsDebug {
    pub suffix: Option<&'static str>,
    pub batched: bool,
    pub panic: bool,
    pub sender: mpsc::UnboundedSender<Instant>,
}

#[derive(Debug)]
struct BackgroundJobDebug {
    name: String,
    start_at: Instant,
    panic: bool,
    sender: mpsc::UnboundedSender<Instant>,
}

impl BackgroundJobDebug {
    fn new(opts: BackgroundJobOptionsDebug) -> Self {
        let mut name = "debug".to_string();
        if let Some(suffix) = opts.suffix {
            write!(&mut name, "_{suffix}").unwrap();
        }

        let start_at = if opts.batched {
            Instant::now() + Duration::from_secs(1)
        } else {
            Instant::now()
        };

        Self {
            name,
            start_at,
            panic: opts.panic,
            sender: opts.sender,
        }
    }
}

#[async_trait]
impl BgJob for BackgroundJobDebug {
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

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    #[tokio::test]
    async fn test_background_jobs() {
        let jobs = JobRunner::new();

        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Start an instantaneous job.
        let start = Instant::now();
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: None,
            batched: false,
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap();

        let end = receiver.recv().await.unwrap();
        assert!(end - start < Duration::from_millis(500));

        // Start a batched job (that's executed after 2 seconds).
        let start = Instant::now();
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: Some("batched"),
            batched: true,
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap();

        // Try to add a new job with the same name immediately.
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: Some("batched"),
            batched: true,
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap();

        let end = receiver.recv().await.unwrap();
        assert!(end - start > Duration::from_secs(1));

        // Can only receive once here.
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

        // Health check by adding a job that panics and another that doesn't.
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: Some("might_panic"),
            batched: false,
            panic: true,
            sender: sender.clone(),
        }))
        .unwrap();

        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: Some("batched"),
            batched: true, // batched so that health checker runs once
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap();

        // Should be able to receive only once (for the valid job).
        let _ = receiver.recv().await.unwrap();
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

        // But we can add another job with the same name that won't panic now.
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: Some("might_panic"),
            batched: false,
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap();

        let _ = receiver.recv().await.unwrap();
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

        // Adding a new batched job should work now.
        let start = Instant::now();
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: Some("batched"),
            batched: true,
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap();

        // Close the job runner (this should wait for all existing jobs to
        // complete).

        // Add another job (to test close).
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: Some("another_batched"),
            batched: true,
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap();

        jobs.close().await.unwrap();

        // Sleep should cancel here and the operation should run very quickly.
        let end1 = receiver.recv().await.unwrap();
        let end2 = receiver.recv().await.unwrap();
        let end = if end1 > end2 { end1 } else { end2 };
        assert!(end - start < Duration::from_millis(500));

        // We shouldn't be able to add jobs now.
        jobs.add(BackgroundJobOptions::Debug(BackgroundJobOptionsDebug {
            suffix: None,
            batched: false,
            panic: false,
            sender: sender.clone(),
        }))
        .unwrap_err();
    }
}
