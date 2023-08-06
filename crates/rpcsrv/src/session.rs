use sqlexec::engine::TrackedSession;
use crate::errors::{ Result, RpcsrvError };
use std::sync::Arc;
use tokio::sync::Mutex;

/// A wrapper around a normal session for sql execution.
#[derive(Clone)]
pub struct RemoteSession {
    /// Inner sql session.
    ///
    /// Wrapped in an Arc and Mutex since the lifetime of the session is not
    /// tied to a single connection, and so needs to be tracked in a shared map.
    session: Arc<Mutex<TrackedSession>>,
}

impl RemoteSession {
    pub fn new(session: TrackedSession) -> Self {
        RemoteSession {
            session: Arc::new(Mutex::new(session)),
        }
    }
}
