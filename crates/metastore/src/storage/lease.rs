//! Object storage based locking.
//!
//! Implementation relies on atomic copy if not exist. GCS supports this.
//! Notably, S3 does not support this. If we choose to deploy to S3, we'll need
//! to tweak this a bit.
//!
//! # System time
//!
//! Leases are currently bounded by an `expires_at` field which gets converted
//! to `SystemTime` during deserialization. `SystemTime` does not guarantee
//! monotonicity, but since we're working with remote system, this doesn't
//! matter. We're making the assumption that every system we're interactining
//! with for locking is has a reasonably accurate clock. Since we should be
//! updating the expiration time with plenty of time spare before the lease
//! actually expirations, a little bit of clock drift doens't matter. We should
//! only be concerned if it's on the order of tens of seconds.

use crate::proto::storage;
use crate::storage::{CatalogStorageObject, Result, StorageError};
use crate::types::storage::{LeaseInformation, LeaseState};
use bytes::BytesMut;
use object_store::{path::Path as ObjectPath, Error as ObjectStoreError, ObjectStore};
use prost::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, error, Instrument};
use uuid::Uuid;

/// Location of the catalog lock object.
const LEASE_INFORMATION_OBJECT: CatalogStorageObject = CatalogStorageObject("lock");

/// How long the lease is held until it's considered expired.
const LEASE_DURATION: Duration = Duration::from_secs(30);

/// How often to renew the lease. This should be significantly less than
/// `LEASE_DURATION`.
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);

/// Locker for locking remote objects.
#[derive(Debug, Clone)]
pub struct RemoteLeaser {
    process_id: Uuid,
    store: Arc<dyn ObjectStore>,
}

impl RemoteLeaser {
    /// Initialize a lease for a database.
    ///
    /// This should only be called when a database is created.
    ///
    /// The initial state of the lease is 'UNLOCKED'. An additional call to
    /// `acquire` is need to grab the lease.
    pub async fn initialize(&self, db_id: &Uuid) -> Result<()> {
        let lease = LeaseInformation {
            state: LeaseState::Unlocked,
            generation: 0,
            held_by: None,
            expires_at: None,
        };

        let proto: storage::LeaseInformation = lease.into();
        let mut bs = BytesMut::new();
        proto.encode(&mut bs)?;

        // Write to temp location first.
        let tmp_path = LEASE_INFORMATION_OBJECT.tmp_object_path(db_id, &self.process_id);
        self.store.put(&tmp_path, bs.freeze()).await?;

        // Try moving it.
        //
        // NOTE: S3 doesn't support atomic copy/rename if not exists.
        let visible_path = LEASE_INFORMATION_OBJECT.visible_object_path(db_id);
        match self
            .store
            .rename_if_not_exists(&tmp_path, &visible_path)
            .await
        {
            Ok(_) => (),
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                return Err(StorageError::FailedInitializationLockExists)
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    /// Acquire the lease for a database.
    ///
    /// Errors if some other process holds the lease.
    pub async fn acquire(&self, db_id: Uuid) -> Result<RemoteLease> {
        // Get initial state of the lock.
        let visible_path = LEASE_INFORMATION_OBJECT.visible_object_path(&db_id);
        let bs = self.store.get(&visible_path).await?.bytes().await?;
        let proto = storage::LeaseInformation::decode(bs)?;
        let lease: LeaseInformation = proto.try_into()?;

        let now = SystemTime::now();

        if lease.state == LeaseState::Locked {
            let held_by = lease
                .held_by
                .ok_or_else(|| StorageError::MissingLeaseField {
                    db_id: db_id.clone(),
                    field: "held_by",
                })?;
            let expires_at = lease
                .expires_at
                .ok_or_else(|| StorageError::MissingLeaseField {
                    db_id: db_id.clone(),
                    field: "expires_at",
                })?;

            // Some other process has the lease.
            if expires_at > now {
                return Err(StorageError::LeaseHeldByOtherProcess {
                    db_id: db_id.clone(),
                    process_id: held_by,
                });
            }

            // Lease was locked, but expired. This should not happen in normal
            // cases. Log an error so we can catch it and investigate.
            error!( prev_held_by = %held_by, acquiring_process = %self.process_id, %db_id,
                    "found expired lease, acquiring lease with new process");

            // Fall through to acquiring...
        }

        // Create the renewer using the current lease information.
        let renewer = LeaseRenewer {
            db_id,
            process_id: self.process_id,
            tmp_path: LEASE_INFORMATION_OBJECT.tmp_object_path(&db_id, &self.process_id),
            visible_path,
            store: self.store.clone(),
        };

        // Immediately "renew" the lease. This will lock the lease for this
        // process.
        let start_generation = renewer.renew_lease(lease.generation).await?;

        Ok(RemoteLease::new(start_generation, renewer)?)
    }
}

/// Locks a catalog in object storage for reading and writing.
pub struct RemoteLease {
    /// Handle for background renews.
    renew_handle: JoinHandle<()>,

    /// Notify the leaser renewer to drop the lease.
    ///
    /// Nested senders to allow awaiting completion of the drop.
    ///
    /// Note that we're not just dropping the handle instead because we want to
    /// ensure that we don't drop in the middle of renewal.
    drop_lease_notifier: mpsc::Sender<oneshot::Sender<Result<()>>>,

    /// If the lease is still valid. This is updated in the background.
    ///
    /// If this is set to `false`, in progress work should be aborted. This
    /// would indicate that we failed to update the lease before it expired.
    valid: Arc<AtomicBool>,
}

impl RemoteLease {
    /// Create a new remote lease, using the provided generation and renewer to
    /// continually renew the lease in the background.
    fn new(start_generation: u64, renewer: LeaseRenewer) -> Result<RemoteLease> {
        let valid = Arc::new(AtomicBool::new(true));
        let (drop_lease_tx, mut drop_lease_rx) = mpsc::channel::<oneshot::Sender<Result<()>>>(1);
        let renew_valid = valid.clone();

        let lease_span =
            debug_span!("lease_renewer", %renewer.db_id, process_id = %renewer.process_id);
        let renew_handle = tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(LEASE_RENEW_INTERVAL);
                let mut generation = start_generation;
                loop {
                    tokio::select! {
                        // Renew lease on interval.
                        _ = interval.tick() => {
                            match renewer.renew_lease(generation).await {
                                Ok(new_generation) => generation = new_generation,
                                Err(e) => {
                                    // We can't guarantee validity at this point.
                                    renew_valid.store(false, Ordering::Relaxed);
                                    error!(%e, "failed to renew lease, exiting background lease renew worker...");
                                    return;
                                }
                            }
                        }

                        // Drop lease on notify.
                        Some(finished_tx) = drop_lease_rx.recv() => {
                            let result =  renewer.drop_lease(generation).await;
                            let _ = finished_tx.send(result);
                        }
                    }
                    interval.tick().await;
                }
            }
            .instrument(lease_span),
        );

        Ok(RemoteLease {
            renew_handle,
            drop_lease_notifier: drop_lease_tx,
            valid,
        })
    }

    /// Returns whether or not the lease is still valid.
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Relaxed)
    }

    /// Drop the lease, immediately making it available for other processes to
    /// acquire.
    ///
    /// If this isn't called, other processes will have to wait for the lease to
    /// expire.
    pub async fn drop_lease(self) -> Result<()> {
        // Can't do anything safely since we don't know the state of the lease.
        if !self.is_valid() {
            return Err(StorageError::UnableToDropLeaseInInvalidState);
        }

        let (done_tx, done_rx) = oneshot::channel();
        if self.drop_lease_notifier.send(done_tx).await.is_err() {
            return Err(StorageError::LeaseRenewerExited);
        }

        match done_rx.await {
            Ok(result) => result,
            Err(_) => Err(StorageError::LeaseRenewerExited),
        }
    }
}

/// Renew and drop leases.
struct LeaseRenewer {
    db_id: Uuid,
    process_id: Uuid,
    /// Where to store the temporary lease object.
    tmp_path: ObjectPath,
    /// Path to the visible lease object.
    visible_path: ObjectPath,
    store: Arc<dyn ObjectStore>,
}

impl LeaseRenewer {
    async fn renew_lease(&self, current_generation: u64) -> Result<u64> {
        let generation = self
            .alter_lease(current_generation, LeaseState::Locked)
            .await?;
        Ok(generation)
    }

    async fn drop_lease(&self, current_generation: u64) -> Result<()> {
        self.alter_lease(current_generation, LeaseState::Unlocked)
            .await?;
        Ok(())
    }

    /// Alter lease with some state.
    ///
    /// Errors if the current generation does not match the generation that's on
    /// the lease, or if we're outside of the lease duration.
    async fn alter_lease(&self, current_generation: u64, state: LeaseState) -> Result<u64> {
        let bs = self.store.get(&self.visible_path).await?.bytes().await?;
        let proto = storage::LeaseInformation::decode(bs)?;
        let lease: LeaseInformation = proto.try_into()?;

        // Means we have a second worker or process updating the lease.
        if lease.generation != current_generation {
            return Err(StorageError::LeaseGenerationMismatch {
                expected: current_generation,
                got: lease.generation,
                held_by: lease.held_by,
            });
        }

        let now = SystemTime::now();

        let expires_at = lease
            .expires_at
            .ok_or_else(|| StorageError::MissingLeaseField {
                db_id: self.db_id,
                field: "expires_at",
            })?;

        if now > expires_at {
            return Err(StorageError::LeaseExpired {
                db_id: self.db_id,
                current: now,
                expired_at: expires_at,
            });
        }

        // Generate new lease with updated state.
        let (generation, _) = lease.generation.overflowing_add(1); // Don't care about overflows.
        let new_lease = if state == LeaseState::Locked {
            // Keeping lease locked.
            LeaseInformation {
                state,
                generation,
                expires_at: Some(now + LEASE_DURATION),
                held_by: Some(self.process_id),
            }
        } else {
            // Unlock lease.
            LeaseInformation {
                state,
                generation,
                expires_at: None,
                held_by: None,
            }
        };

        // Write to storage.
        let proto: storage::LeaseInformation = new_lease.into();
        let mut bs = BytesMut::new();
        proto.encode(&mut bs)?;
        self.store.put(&self.tmp_path, bs.freeze()).await?;

        // Rename...
        //
        // Note that this has a chance of overwriting a newly acquired lease.
        // The background worker checking lease validity should help with
        // detecting these cases, but it's not full-proof. Eventually we'll
        // want to look into object versioning for gcs.
        //
        // See <https://cloud.google.com/storage/docs/object-versioning>
        self.store
            .rename(&self.tmp_path, &self.visible_path)
            .await?;

        Ok(generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store_util::temp::TempObjectStore;

    async fn insert_lease(store: &dyn ObjectStore, path: &ObjectPath, lease: LeaseInformation) {
        let proto: storage::LeaseInformation = lease.into();
        let mut bs = BytesMut::new();
        proto.encode(&mut bs).unwrap();
        store.put(&path, bs.freeze()).await.unwrap();
    }

    async fn get_lease(store: &dyn ObjectStore, path: &ObjectPath) -> LeaseInformation {
        let bs = store.get(path).await.unwrap().bytes().await.unwrap();
        let proto = storage::LeaseInformation::decode(bs).unwrap();
        proto.try_into().unwrap()
    }

    struct TestParams {
        now: SystemTime,
        process_id: Uuid,
        db_id: Uuid,
        store: Arc<dyn ObjectStore>,
        tmp_path: ObjectPath,
        visible_path: ObjectPath,
    }

    impl TestParams {
        /// Create a new renewer using params.
        fn renewer(&self) -> LeaseRenewer {
            LeaseRenewer {
                db_id: self.db_id,
                process_id: self.process_id,
                tmp_path: self.tmp_path.clone(),
                visible_path: self.visible_path.clone(),
                store: self.store.clone(),
            }
        }
    }

    impl Default for TestParams {
        fn default() -> Self {
            let process_id = Uuid::new_v4();
            let db_id = Uuid::new_v4();
            TestParams {
                now: SystemTime::now(),
                process_id,
                db_id,
                store: Arc::new(TempObjectStore::new().unwrap()),
                tmp_path: LEASE_INFORMATION_OBJECT.tmp_object_path(&db_id, &process_id),
                visible_path: LEASE_INFORMATION_OBJECT.visible_object_path(&db_id),
            }
        }
    }

    #[tokio::test]
    async fn lease_renewer_renew_simple() {
        let params = TestParams::default();

        let start = LeaseInformation {
            state: LeaseState::Locked,
            generation: 1,
            expires_at: Some(params.now + LEASE_DURATION),
            held_by: Some(params.process_id),
        };

        // Insert first lease.
        insert_lease(params.store.as_ref(), &params.visible_path, start).await;

        let renewer = params.renewer();

        // Renew lease using the first generation.
        let new_generation = renewer.renew_lease(1).await.unwrap();
        assert_eq!(2, new_generation);

        let new_lease = get_lease(params.store.as_ref(), &params.visible_path).await;
        assert_eq!(2, new_lease.generation);
        assert_eq!(LeaseState::Locked, new_lease.state);
        assert!(new_lease.expires_at.unwrap() > params.now); // Possibly flaky.
        assert_eq!(params.process_id, new_lease.held_by.unwrap());
    }

    #[tokio::test]
    async fn lease_renewer_renew_unexpected_generation() {
        let params = TestParams::default();

        let start = LeaseInformation {
            state: LeaseState::Locked,
            generation: 2, // Mocking some other process came in and started messing with the lease.
            expires_at: Some(params.now + LEASE_DURATION),
            held_by: Some(params.process_id),
        };

        insert_lease(params.store.as_ref(), &params.visible_path, start).await;

        let renewer = params.renewer();

        // Try to renew with older generation.
        let _ = renewer.renew_lease(1).await.unwrap_err();
    }

    #[tokio::test]
    async fn lease_renewer_renew_outside_of_expiration() {
        let params = TestParams::default();

        let start = LeaseInformation {
            state: LeaseState::Locked,
            generation: 1,
            expires_at: Some(params.now - Duration::from_secs(1)), // Mark lease as already expired.
            held_by: Some(params.process_id),
        };

        insert_lease(params.store.as_ref(), &params.visible_path, start).await;

        let renewer = params.renewer();

        // Try to renew with older generation.
        let _ = renewer.renew_lease(1).await.unwrap_err();
    }

    #[tokio::test]
    async fn lease_renewer_drop_lease() {
        let params = TestParams::default();

        let start = LeaseInformation {
            state: LeaseState::Locked,
            generation: 1,
            expires_at: Some(params.now + LEASE_DURATION),
            held_by: Some(params.process_id),
        };
        insert_lease(params.store.as_ref(), &params.visible_path, start).await;

        let renewer = params.renewer();

        let new_generation = renewer.renew_lease(1).await.unwrap();

        // Now try to drop it.
        renewer.drop_lease(new_generation).await.unwrap();

        let lease = get_lease(params.store.as_ref(), &params.visible_path).await;
        assert_eq!(LeaseState::Unlocked, lease.state);
        assert_eq!(3, lease.generation);
        assert_eq!(None, lease.expires_at);
        assert_eq!(None, lease.held_by);
    }
}
