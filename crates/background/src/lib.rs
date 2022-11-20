//! Background jobs.
pub mod errors;
pub mod storage;

use access::runtime::AccessRuntime;
use cloud::client::CloudClient;
use common::background::BackgroundConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, debug_span, error, info, Instrument};

use storage::{DatabaseStorageUsageJob, DatabaseStorageUsageSink};

const DEBUG_DURATION: Duration = Duration::from_secs(60);

/// Run all background jobs on periodic intervals.
#[derive(Debug)]
pub struct BackgroundWorker {
    conf: BackgroundConfig,
    cloud_client: Option<Arc<CloudClient>>,
    runtime: Arc<AccessRuntime>,
}

impl BackgroundWorker {
    pub fn new(
        conf: BackgroundConfig,
        cloud_client: Option<Arc<CloudClient>>,
        runtime: Arc<AccessRuntime>,
    ) -> BackgroundWorker {
        BackgroundWorker {
            conf,
            cloud_client,
            runtime,
        }
    }

    /// Begin the background worker.
    ///
    /// Note that this handles all errors internally. Errors should not stop the
    /// worker from continuing to process jobs.
    pub async fn begin(self) {
        debug!("starting background worker");
        let usage_job = DatabaseStorageUsageJob::new(self.runtime.clone());
        let usage_sink = DatabaseStorageUsageSink::new(self.cloud_client.clone());
        let mut usage_interval = interval_with_skipped_ticks(self.conf.storage_reporting.interval);

        let mut debug_interval = interval_with_skipped_ticks(DEBUG_DURATION);

        loop {
            tokio::select! {
                _ = usage_interval.tick() => Self::handle_usage_job(&usage_job, &usage_sink).await,
                _ = debug_interval.tick() => debug!("debug interval hit"),
            }
        }
    }

    async fn handle_usage_job(job: &DatabaseStorageUsageJob, sink: &DatabaseStorageUsageSink) {
        let span = debug_span!("database_storage_usage_job");
        async move {
            match job.compute_storage_total_bytes().await {
                Ok(usage_bytes) => {
                    info!(%usage_bytes, "total storage used");
                    if let Err(e) = sink.send_usage(usage_bytes).await {
                        error!(%e, "failed to send usage bytes to sink");
                    }
                }
                Err(e) => {
                    error!(%e, "failed to compute total storage usage");
                }
            }
        }
        .instrument(span)
        .await;
    }
}

/// Create an interval with skipped ticks.
///
/// By default, intervals will "burst" to catch up if ticks are skipped. Since
/// our jobs may be somewhat long lived, bursting is not ideal for us as it
/// could lead to background jobs doing a lot of expensive and useless work.
fn interval_with_skipped_ticks(dur: Duration) -> Interval {
    let mut interval = tokio::time::interval(dur);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}
