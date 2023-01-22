//! Metastore persistent storage.

pub mod transaction;

mod lease;

use transaction::StorageTransaction;

use crate::types::catalog::CatalogEntry;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Failed to initialize database lease, lease already exists.")]
    FailedInitializationLockExists,

    #[error("Lease for database '{db_id}' held by other process; process_id: {process_id}")]
    LeaseHeldByOtherProcess { db_id: Uuid, process_id: Uuid },

    #[error("Missing field on lease for database; db_id: {db_id}, field: {field}")]
    MissingLeaseField { db_id: Uuid, field: &'static str },

    #[error("Lease generation doesn't match expected; expected: {expected}, got: {got}, held_by: {held_by:?}")]
    LeaseGenerationMismatch {
        expected: u64,
        got: u64,
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

    #[error("Unable to drop lease in invalid state.")]
    UnableToDropLeaseInInvalidState,

    #[error("Lease renewer exited.")]
    LeaseRenewerExited,

    #[error("Duplicate key for insert: {0}")]
    DuplicateKey(String),

    #[error(transparent)]
    ProtoConv(#[from] crate::types::ProtoConvError),

    #[error("Failed to encode protobuf for storage: {0}")]
    ProstEncode(#[from] prost::EncodeError),

    #[error("Failed to decode protobuf from storage: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

/// Describes a catalog object in object storage at a static path.
#[derive(Debug, Clone, Copy)]
pub struct CatalogStorageObject(&'static str);

impl CatalogStorageObject {
    /// Get the visible object path of this file for a database. Objects at this
    /// path will be visible to other processes.
    ///
    /// Path format: 'database/<db_id>/visible/<object_name>'
    ///
    /// Scanning all visible objects only need to do a prefix scan on
    /// 'database/<db_id>/visible/'. Temporary objects will not be included in
    /// such a scan.
    fn visible_object_path(&self, db_id: &Uuid) -> ObjectPath {
        ObjectPath::from(format!(
            "databases/{}/visible/{}",
            db_id.to_string(),
            self.0
        ))
    }

    /// Get a temporary path to use for this process.
    ///
    /// Path format: 'database/<db_id>/tmp/<proc_id>/<object_name>'
    fn tmp_object_path(&self, db_id: &Uuid, process_id: &Uuid) -> ObjectPath {
        ObjectPath::from(format!(
            "databases/{}/tmp/{}/{}",
            db_id.to_string(),
            self.0,
            process_id
        ))
    }
}

const METADATA_OBJECT: CatalogStorageObject = CatalogStorageObject("metadata");
const CATALOG_BLOB_OBJECT: CatalogStorageObject = CatalogStorageObject("catalog");

pub struct Storage {
    store: Arc<dyn ObjectStore>,
}

impl Storage {
    /// Read a database's catalog state from object storage.
    pub async fn read_database_state(
        &self,
        db_id: Uuid,
    ) -> Result<Option<HashMap<u32, CatalogEntry>>> {
        unimplemented!()
    }

    /// Try to commit a transaction against object storage.
    pub async fn try_commit(
        &self,
        db_id: Uuid,
        tx: StorageTransaction<u32, CatalogEntry>,
    ) -> Result<()> {
        unimplemented!()
    }
}
