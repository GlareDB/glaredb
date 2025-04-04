use std::fmt::Debug;

use super::filesystem::dispatch::FileSystemDispatch;
use super::time::RuntimeInstant;

/// Provides sytem dependencies.
// TODO: Do I want Clone?
// TODO: Should this be global to the engine or per-session
pub trait SystemRuntime: Debug + Sync + Send + Clone + 'static {
    /// The time instant type to use.
    type Instant: RuntimeInstant;

    /// Get a reference the filesystem dispatch.
    fn filesystem_dispatch(&self) -> &FileSystemDispatch;
}
