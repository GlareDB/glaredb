//! Metastore persistent storage.

pub mod persist;

mod lease;

use object_store::path::Path as ObjectPath;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Lease for database '{db_id}' held by other process; other_process_id: {other_process_id}, current_process_id: {current_process_id}")]
    LeaseHeldByOtherProcess {
        db_id: Uuid,
        other_process_id: Uuid,
        current_process_id: Uuid,
    },

    #[error("Missing field on lease for database; db_id: {db_id}, field: {field}")]
    MissingLeaseField { db_id: Uuid, field: &'static str },

    #[error("Lease generation doesn't match expected; expected: {expected}, have: {have}, held_by: {held_by:?}")]
    LeaseGenerationMismatch {
        expected: u64,
        have: u64,
        held_by: Option<Uuid>,
    },

    #[error(
        "Lease for database '{db_id}' expired; current: {current:?}, expired_at: {expired_at:?}"
    )]
    LeaseExpired {
        db_id: Uuid,
        current: SystemTime,
        expired_at: SystemTime,
    },

    #[error("Attempted to write to the catalog with an out of date version; expected: {expected}, have: {have}")]
    AttemptedOutOfDataCatalogWrite { expected: u64, have: u64 },

    #[error("Lease not valid for database: {db_id}")]
    LeaseNotValid { db_id: Uuid },

    #[error("Unable to drop lease in invalid state.")]
    UnableToDropLeaseInInvalidState,

    #[error("Lease renewer exited.")]
    LeaseRenewerExited,

    #[error(transparent)]
    ProtoConv(#[from] protogen::errors::ProtoConvError),

    #[error("Failed to encode protobuf for storage: {0}")]
    ProstEncode(#[from] prost::EncodeError),

    #[error("Failed to decode protobuf from storage: {0}")]
    ProstDecode(#[from] prost::DecodeError),
}

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

pub trait StorageObject<S: AsRef<str>> {
    /// The name of the storage object.
    fn object_name(&self) -> S;

    /// Get a temporary path to use for this process.
    ///
    /// Path format: 'database/<db_id>/tmp/<proc_id>/<object_name>'
    fn tmp_path(&self, db_id: &Uuid, process_id: &Uuid) -> ObjectPath {
        ObjectPath::from(format!(
            "databases/{}/tmp/{}/{}",
            db_id,
            process_id,
            self.object_name().as_ref(),
        ))
    }

    /// Get the visible object path of this file for a database. Objects at this
    /// path will be visible to other processes.
    ///
    /// Path format: 'database/<db_id>/visible/<object_name>'
    ///
    /// Scanning all visible objects only need to do a prefix scan on
    /// 'database/<db_id>/visible/'. Temporary objects will not be included in
    /// such a scan.
    fn visible_path(&self, db_id: &Uuid) -> ObjectPath {
        ObjectPath::from(format!(
            "databases/{}/visible/{}",
            db_id,
            self.object_name().as_ref(),
        ))
    }
}

/// An object that hase only one version for a database.
#[derive(Debug, Clone, Copy)]
pub struct SingletonStorageObject(&'static str);

impl StorageObject<&'static str> for SingletonStorageObject {
    fn object_name(&self) -> &'static str {
        self.0
    }
}

/// An object that has multiple versions.
#[derive(Debug, Clone, Copy)]
pub struct VersionedStorageObject(&'static str, u64);

impl VersionedStorageObject {
    pub fn with_version(&self, version: u64) -> VersionedStorageObject {
        VersionedStorageObject(self.0, version)
    }
}

impl StorageObject<String> for VersionedStorageObject {
    fn object_name(&self) -> String {
        format!("{}.{}", self.0, self.1)
    }
}
