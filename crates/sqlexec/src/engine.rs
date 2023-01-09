use crate::catalog::Catalog;
use crate::errors::{ExecError, Result};
use crate::session::Session;
use parking_lot::RwLock;
use rand::Rng;
use stablestore::StableStorage;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

/// Wrapper around the database catalog.
pub struct Engine {
    catalog: Arc<Catalog>,
    pid_gen: AtomicI32,
    /// Channels to send cancellation requests on.
    session_cancels: RwLock<BTreeMap<SessionCancelKey, Arc<watch::Sender<()>>>>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new<S: StableStorage>(_storage: S) -> Result<Engine> {
        let catalog = Catalog::open().await?;
        Ok(Engine {
            catalog: Arc::new(catalog),
            pid_gen: AtomicI32::new(1),
            session_cancels: RwLock::new(BTreeMap::new()),
        })
    }

    /// Create a new session with the given id.
    ///
    /// The returned cancel token should be sent to the client to allow
    /// canceling operations later.
    pub fn new_session(&self, id: Uuid) -> Result<(Session, SessionCancelKey)> {
        let pid = self.pid_gen.fetch_add(1, Ordering::Relaxed);
        let sess = Session::new(self.catalog.clone(), id)?;
        let key = SessionCancelKey::new(pid);

        {
            let mut cancels = self.session_cancels.write();
            if cancels.contains_key(&key) {
                return Err(ExecError::DuplicatePid(pid));
            }
            cancels.insert(key, sess.get_cancel_tx());
        }

        Ok((sess, key))
    }
}

/// Cancel key used to attempt to cancel in-progress session operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionCancelKey {
    pub pid: i32,
    pub secret: i32,
}

impl SessionCancelKey {
    fn new(pid: i32) -> SessionCancelKey {
        let mut rng = rand::thread_rng();
        let secret: i32 = rng.gen();
        SessionCancelKey { pid, secret }
    }
}
