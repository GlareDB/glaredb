use crate::errors::Result;
use crate::BackgroundJob;
use async_trait::async_trait;
use cloud::client::CloudClient;
use futures::TryStreamExt;
use object_store::ObjectStore;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, debug_span, info, Instrument};

/// Background job for computing the total object storage usage of this database
/// and sending it to cloud.
///
/// NOTE: It's likely you'll want to be using the `PrefixObjectStore` to only
/// count files that belong to a single database.
///
/// NOTE: This may be expanded in the future to hold the total in some in-memory
/// structure such that it's accessible through the catalog.
#[derive(Debug)]
pub struct ObjectStorageUsageJob {
    store: Arc<dyn ObjectStore>,
    client: Option<Arc<CloudClient>>,
    interval_dur: Duration,
}

impl ObjectStorageUsageJob {
    /// Create a new worker for computing storage usage and sending it to Cloud.
    ///
    /// If client is `None`, no attempt will be made to actually send to Cloud,
    /// and the total usage will just be logged.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        client: Option<Arc<CloudClient>>,
        dur: Duration,
    ) -> Self {
        ObjectStorageUsageJob {
            store,
            client,
            interval_dur: dur,
        }
    }

    /// Compute the total storage in bytes that this database is taking up in
    /// object store.
    async fn compute_storage_total_bytes(&self) -> Result<u64> {
        debug!(store = %self.store, "computing storage usage");
        let stream = self.store.list(None).await?;
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

#[async_trait]
impl BackgroundJob for ObjectStorageUsageJob {
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

    async fn execute(&self) -> Result<()> {
        let span = debug_span!("database_storage_usage_job");
        async move {
            match self.compute_storage_total_bytes().await {
                Ok(usage_bytes) => {
                    info!(%usage_bytes, "total storage used");
                    self.send_usage(usage_bytes).await
                }
                Err(e) => Err(e),
            }
        }
        .instrument(span)
        .await?;
        Ok(())
    }
}

impl fmt::Display for ObjectStorageUsageJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabaseStorageJob")
    }
}
