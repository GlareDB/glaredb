use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use glaredb_error::{DbError, Result};
use rayexec_io::http::HttpClient;

use super::time::RuntimeInstant;
use crate::io::file::FileOpener;

/// IO dependendencies.
pub trait IoRuntime: Debug + Sync + Send + Clone + 'static {
    type HttpClient: HttpClient;
    type FileProvider: FileOpener;
    type Instant: RuntimeInstant; // TODO: Should this be on the runtime?

    /// Returns a file provider.
    fn file_provider(&self) -> Arc<Self::FileProvider>;

    /// Returns an http client. Freely cloneable.
    fn http_client(&self) -> Self::HttpClient;

    fn as_dyn_io_runtime(&self) -> DynIoRuntime {
        DynIoRuntime::from_io_runtime(self)
    }
}

#[derive(Debug, Clone, Copy)]
struct DynIoRuntimeVTable {}

#[derive(Debug, Clone)]
pub struct DynIoRuntime<'a> {
    runtime: &'a (dyn Any + Sync + Send),
    _vtable: &'static DynIoRuntimeVTable,
}

impl<'a> DynIoRuntime<'a> {
    pub fn from_io_runtime<R>(runtime: &'a R) -> Self
    where
        R: IoRuntime,
    {
        DynIoRuntime {
            runtime: runtime as _,
            _vtable: R::VTABLE,
        }
    }

    pub fn downcast<R>(&self) -> Result<&R>
    where
        R: IoRuntime,
    {
        self.runtime
            .downcast_ref::<R>()
            .ok_or_else(|| DbError::new("Unexpected IO runtime"))
    }
}

trait IoRuntimeVTable {
    const VTABLE: &'static DynIoRuntimeVTable;
}

impl<R> IoRuntimeVTable for R
where
    R: IoRuntime,
{
    const VTABLE: &'static DynIoRuntimeVTable = &DynIoRuntimeVTable {};
}
