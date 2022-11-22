use crate::errors::Result;
use crate::BackgroundJob;
use access::runtime::AccessRuntime;
use cloud::client::CloudClient;
use futures::TryStreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, debug_span, error, info, Instrument};

/// Background job for computing total storage usage of this database and
/// sending it to cloud.
///
/// NOTE: This may be expanded in the future to hold the total in some in-memory
/// structure such that it's accessible through the catalog.
#[derive(Debug)]
pub struct DatabaseStorageUsageJob {
    runtime: Arc<AccessRuntime>,
    client: Option<Arc<CloudClient>>,
    interval_dur: Duration,
}

impl DatabaseStorageUsageJob {
    /// Create a new worker for computing storage usage and sending it to Cloud.
    ///
    /// If client is `None`, no attempt will be made to actually send to Cloud,
    /// and the total usage will just be logged.
    pub fn new(
        runtime: Arc<AccessRuntime>,
        client: Option<Arc<CloudClient>>,
        dur: Duration,
    ) -> Self {
        DatabaseStorageUsageJob {
            runtime,
            client,
            interval_dur: dur,
        }
    }

    /// Compute the total storage in bytes that this database is taking up in
    /// object store.
    async fn compute_storage_total_bytes(&self) -> Result<u64> {
        let prefix = self.runtime.object_path_prefix();
        debug!(%prefix, "computing storage usage with prefix");
        let stream = self.runtime.object_store().list(Some(prefix)).await?;
        let total = stream
            .try_fold(0, |acc, meta| async move { Ok(acc + meta.size) })
            .await?;
        Ok(total as u64)
    }

    /// Send storage usage to cloud if available.
    async fn send_usage(&self, usage_bytes: u64) -> Result<()> {
        match &self.client {
            Some(client) => {
                client.report_usage(usage_bytes).await?;
            }
            None => {
                debug!("skipping sending storage usage to Cloud");
            }
        }
        Ok(())
    }
}

impl BackgroundJob for DatabaseStorageUsageJob {
    fn interval(&self) -> Interval {
        // Skip missed ticks instead of bursting to catch up.
        //
        // Calculating the total storage used for the database should not exceed
        // this interval, but if it does, we should not try to burst as it could
        // start to overload the system.
        let mut interval = tokio::time::interval(self.interval_dur);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval
    }

    fn job_fut(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let span = debug_span!("database_storage_usage_job");
            async move {
                match self.compute_storage_total_bytes().await {
                    Ok(usage_bytes) => {
                        info!(%usage_bytes, "total storage used");
                        if let Err(e) = self.send_usage(usage_bytes).await {
                            error!(%e, "failed to send usage bytes to sink");
                        }
                    }
                    Err(e) => {
                        error!(%e, "failed to compute total storage usage");
                    }
                }
            }
            .instrument(span)
            .await
        })
    }
}
