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
//! matter. We're making the assumption that every system we're interacting with
//! for locking has a reasonably accurate clock. Since we should be updating
//! the expiration time with plenty of time spare before the lease actually
//! expires, a little bit of clock drift doesn't matter. We should only be
//! concerned if it's on the order of tens of seconds.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::BytesMut;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, ObjectStore};
use prost::Message;
use protogen::gen::metastore::storage;
use protogen::metastore::types::storage::{LeaseInformation, LeaseState};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, error, Instrument};
use uuid::Uuid;

use crate::storage::{Result, SingletonStorageObject, StorageError, StorageObject};

/// Location of the catalog lock object.
const LEASE_INFORMATION_OBJECT: SingletonStorageObject = SingletonStorageObject("lease");

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
    /// Create a new remote leaser to the given object store.
    pub fn new(process_id: Uuid, store: Arc<dyn ObjectStore>) -> RemoteLeaser {
        RemoteLeaser { process_id, store }
    }

    /// Initialize a lease for a database.
    ///
    /// Idempotent for a catalog. If a lease already exists, its state is
    /// unchanged.
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
        let tmp_path = LEASE_INFORMATION_OBJECT.tmp_path(db_id, &self.process_id);
        self.store.put(&tmp_path, bs.freeze()).await?;

        // Try moving it.
        //
        // NOTE: S3 doesn't support atomic copy/rename if not exists.
        let visible_path = LEASE_INFORMATION_OBJECT.visible_path(db_id);
        match self
            .store
            .rename_if_not_exists(&tmp_path, &visible_path)
            .await
        {
            Ok(_) => (),
            Err(ObjectStoreError::AlreadyExists { .. }) => return Ok(()),
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    /// Acquire the lease for a database.
    ///
    /// Errors if some other process holds the lease.
    pub async fn acquire(&self, db_id: Uuid) -> Result<RemoteLease> {
        let visible_path = LEASE_INFORMATION_OBJECT.visible_path(&db_id);
        let renewer = LeaseRenewer {
            db_id,
            process_id: self.process_id,
            visible_path,
            store: self.store.clone(),
        };

        // Try to acquire.
        let start_generation = renewer.acquire_lease().await?;

        RemoteLease::new(start_generation, renewer)
    }
}

/// Locks a catalog in object storage for writing.
#[derive(Debug)]
pub struct RemoteLease {
    /// Handle for background renews.
    _renew_handle: JoinHandle<()>,

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
                            // Exit loop, no longer need to be doing any work.
                            return;
                        }
                    }
                }
            }
            .instrument(lease_span),
        );

        Ok(RemoteLease {
            _renew_handle: renew_handle,
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
    /// Path to the visible lease object.
    visible_path: ObjectPath,
    store: Arc<dyn ObjectStore>,
}

impl LeaseRenewer {
    /// Acquire the lease if it's available. Errors if it's not available.
    async fn acquire_lease(&self) -> Result<u64> {
        let lease = self.read_lease(None).await?;
        let now = SystemTime::now();

        if lease.state == LeaseState::Locked {
            let held_by = lease.held_by.ok_or(StorageError::MissingLeaseField {
                db_id: self.db_id,
                field: "held_by",
            })?;
            let expires_at = lease.expires_at.ok_or(StorageError::MissingLeaseField {
                db_id: self.db_id,
                field: "expires_at",
            })?;

            // Some other process has the lease.
            if expires_at > now && held_by != self.process_id {
                return Err(StorageError::LeaseHeldByOtherProcess {
                    db_id: self.db_id,
                    other_process_id: held_by,
                    current_process_id: self.process_id,
                });
            }

            // Lease was locked, but expired. This should not happen in normal
            // cases. Log an error so we can catch it and investigate.
            error!( prev_held_by = %held_by, acquiring_process = %self.process_id, db_id = %self.db_id,
                    "found expired lease, acquiring lease with new process");

            // Fall through to acquiring...
        }

        let (generation, _) = lease.generation.overflowing_add(1); // Don't care about overflows.
        let new_lease = LeaseInformation {
            state: LeaseState::Locked,
            generation,
            expires_at: Some(now + LEASE_DURATION),
            held_by: Some(self.process_id),
        };

        self.write_lease(new_lease).await?;

        Ok(generation)
    }

    /// Renew a lease.
    async fn renew_lease(&self, current_generation: u64) -> Result<u64> {
        let lease = self.read_lease(Some(current_generation)).await?;
        let now = SystemTime::now();

        let expires_at = lease.expires_at.ok_or(StorageError::MissingLeaseField {
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

        let (generation, _) = lease.generation.overflowing_add(1); // Don't care about overflows.
        let new_lease = LeaseInformation {
            state: LeaseState::Locked,
            generation,
            expires_at: Some(now + LEASE_DURATION),
            held_by: Some(self.process_id),
        };

        self.write_lease(new_lease).await?;

        Ok(generation)
    }

    /// Drop the lease.
    async fn drop_lease(&self, current_generation: u64) -> Result<()> {
        let lease = self.read_lease(Some(current_generation)).await?;

        let (generation, _) = lease.generation.overflowing_add(1); // Don't care about overflows.
        let new_lease = LeaseInformation {
            state: LeaseState::Unlocked,
            generation,
            expires_at: None,
            held_by: None,
        };

        self.write_lease(new_lease).await?;

        Ok(())
    }

    /// Read the lease, checking that it's the generation we expect if it's provided.
    async fn read_lease(&self, current_generation: Option<u64>) -> Result<LeaseInformation> {
        let bs = self.store.get(&self.visible_path).await?.bytes().await?;
        let proto = storage::LeaseInformation::decode(bs)?;
        let lease: LeaseInformation = proto.try_into()?;

        if let Some(current_generation) = current_generation {
            // Means we have a second worker or process updating the lease.
            if lease.generation != current_generation {
                return Err(StorageError::LeaseGenerationMismatch {
                    expected: current_generation,
                    have: lease.generation,
                    held_by: lease.held_by,
                });
            }
        }

        Ok(lease)
    }

    async fn try_write_lease(&self, lease: LeaseInformation) -> Result<()> {
        // Write to storage.
        let proto: storage::LeaseInformation = lease.into();
        let mut bs = BytesMut::new();
        proto.encode(&mut bs)?;

        let tmp_path = LEASE_INFORMATION_OBJECT.tmp_path(&self.db_id, &self.process_id);

        self.store.put(&tmp_path, bs.freeze()).await?;

        // Rename...
        //
        // Note that this has a chance of overwriting a newly acquired lease.
        // The background worker checking lease validity should help with
        // detecting these cases, but it's not full-proof. Eventually we'll
        // want to look into object versioning for gcs.
        //
        // See <https://cloud.google.com/storage/docs/object-versioning>
        self.store.rename(&tmp_path, &self.visible_path).await?;

        Ok(())
    }

    async fn write_lease(&self, lease: LeaseInformation) -> Result<()> {
        let mut final_err = String::new();

        for num_try in 1..=5 {
            if let Err(err) = self.try_write_lease(lease.clone()).await {
                final_err = err.to_string();

                if let StorageError::ObjectStore(err) = &err {
                    if let ObjectStoreError::Generic { store, source } = err {
                        let source = source.to_string();
                        // Error code 429 is not handled by object_store's retry
                        // configuration. We should maybe upstream to catch this
                        // and retry properly.
                        if store == &"GCS" && source.contains("429") {
                            debug!(%err, %num_try, "hit rate limit for writing lease object");
                            // Sleep for some time (half the time of when the
                            // request wouldn't throttle) before trying to write
                            // the lease.
                            //
                            // See: https://cloud.google.com/storage/docs/objects#immutability
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    }
                }

                return Err(err);
            } else {
                return Ok(());
            }
        }

        // Return the original error if failed after retries.
        Err(StorageError::LeaseWriteError(final_err))
    }
}

#[cfg(test)]
mod tests {
    use object_store_util::temp::TempObjectStore;

    use super::*;

    async fn insert_lease(store: &dyn ObjectStore, path: &ObjectPath, lease: LeaseInformation) {
        let proto: storage::LeaseInformation = lease.into();
        let mut bs = BytesMut::new();
        proto.encode(&mut bs).unwrap();
        store.put(path, bs.freeze()).await.unwrap();
    }

    async fn get_lease(store: &dyn ObjectStore, path: &ObjectPath) -> LeaseInformation {
        let bs = store.get(path).await.unwrap().bytes().await.unwrap();
        let proto = storage::LeaseInformation::decode(bs).unwrap();
        proto.try_into().unwrap()
    }

    // Note that the implementation for default uses non-zero uuids.
    #[derive(Debug, Clone)]
    struct LeaseRenewerTestParams {
        now: SystemTime,
        process_id: Uuid,
        db_id: Uuid,
        store: Arc<dyn ObjectStore>,
        visible_path: ObjectPath,
    }

    impl LeaseRenewerTestParams {
        /// Create a new renewer using params.
        fn renewer(&self) -> LeaseRenewer {
            LeaseRenewer {
                db_id: self.db_id,
                process_id: self.process_id,
                visible_path: self.visible_path.clone(),
                store: self.store.clone(),
            }
        }
    }

    impl Default for LeaseRenewerTestParams {
        fn default() -> Self {
            let process_id = Uuid::new_v4();
            let db_id = Uuid::new_v4();
            LeaseRenewerTestParams {
                now: SystemTime::now(),
                process_id,
                db_id,
                store: Arc::new(TempObjectStore::new().unwrap()),
                visible_path: LEASE_INFORMATION_OBJECT.visible_path(&db_id),
            }
        }
    }

    #[tokio::test]
    async fn lease_renewer_renew_simple() {
        let params = LeaseRenewerTestParams::default();

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
        let params = LeaseRenewerTestParams::default();

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
        let params = LeaseRenewerTestParams::default();

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
        let params = LeaseRenewerTestParams::default();

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

    #[tokio::test]
    async fn lease_renewer_reacquire_own_lease() {
        // See second error message in https://github.com/GlareDB/cloud/issues/2746
        //
        // If this process has already acquired the lease, attempting to acquire
        // it again from the same process should work. This may happen if the
        // metastore hits an error writing to object store (rate limit), and so
        // fails the full request without actually being able to unlock the
        // lease. Attempting to reacquire the lease in a second request should
        // work though, since we still have the lease.
        let params = LeaseRenewerTestParams::default();

        let start = LeaseInformation {
            state: LeaseState::Locked,
            generation: 1,
            expires_at: Some(params.now + LEASE_DURATION),
            held_by: Some(params.process_id),
        };
        insert_lease(params.store.as_ref(), &params.visible_path, start).await;

        let renewer = params.renewer();

        // Make sure we can acquire it even though it's already been "acquired"
        // (active lease inserted).
        renewer.acquire_lease().await.unwrap();
    }

    #[tokio::test]
    async fn remote_lease_idempotent_initialize() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let process_id = Uuid::new_v4();
        let leaser = RemoteLeaser::new(process_id, store);

        let db_id = Uuid::new_v4();
        leaser.initialize(&db_id).await.unwrap();
        leaser.initialize(&db_id).await.unwrap();
    }

    #[tokio::test]
    async fn remote_leaser_acquire_drop() {
        // TODO: Local filesystem object store doesn't handle renames/copies
        // correctly.
        let store = Arc::new(object_store::memory::InMemory::new());
        let leaser = RemoteLeaser::new(Uuid::new_v4(), store);

        let db_id = Uuid::new_v4();
        leaser.initialize(&db_id).await.unwrap();
        let lease = leaser.acquire(db_id).await.unwrap();

        assert!(lease.is_valid());

        lease.drop_lease().await.unwrap();
    }

    #[tokio::test]
    async fn remote_leaser_fail_acquire() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let process_id = Uuid::new_v4();
        let leaser = RemoteLeaser::new(process_id, store.clone());

        let db_id = Uuid::new_v4();
        leaser.initialize(&db_id).await.unwrap();

        let active = leaser.acquire(db_id).await.unwrap();

        // Try to acquire the lease using a different leaser.
        let different = RemoteLeaser::new(Uuid::new_v4(), store);
        let result = different.acquire(db_id).await;
        assert!(
            matches!(result, Err(StorageError::LeaseHeldByOtherProcess { .. })),
            "result: {:?}",
            result,
        );

        // Dropping previous lease should allow us to acquire a new one in the
        // other process.
        active.drop_lease().await.unwrap();

        let _ = different.acquire(db_id).await.unwrap();
    }
}
