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
    type TokioHandle: TokioHandlerProvider;
    type Instant: RuntimeInstant; // TODO: Should this be on the runtime?

    /// Returns a file provider.
    fn file_provider(&self) -> Arc<Self::FileProvider>;

    /// Returns an http client. Freely cloneable.
    fn http_client(&self) -> Self::HttpClient;

    /// Return a handle to a tokio runtime if this execution runtime has a tokio
    /// runtime configured.
    ///
    /// This is needed because our native execution runtime does not depend on
    /// tokio, but certain libraries and drivers that we want to use have an
    /// unavoidable dependency on tokio.
    ///
    /// Data sources should error if they require tokio and if this returns
    /// None.
    fn tokio_handle(&self) -> &Self::TokioHandle;

    fn as_dyn_io_runtime(&self) -> DynIoRuntime {
        DynIoRuntime::from_io_runtime(self)
    }
}

pub trait TokioHandlerProvider {
    fn handle_opt(&self) -> Option<tokio::runtime::Handle>;

    fn handle(&self) -> Result<tokio::runtime::Handle> {
        self.handle_opt()
            .ok_or_else(|| DbError::new("Tokio runtime not configured"))
    }
}

#[derive(Debug)]
pub struct OptionalTokioRuntime(Option<tokio::runtime::Runtime>);

impl OptionalTokioRuntime {
    pub fn new(runtime: Option<tokio::runtime::Runtime>) -> Self {
        OptionalTokioRuntime(runtime)
    }
}

impl TokioHandlerProvider for OptionalTokioRuntime {
    fn handle_opt(&self) -> Option<tokio::runtime::Handle> {
        self.0.as_ref().map(|t| t.handle().clone())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DynIoRuntimeVTable {}

#[derive(Debug, Clone)]
pub struct DynIoRuntime<'a> {
    runtime: &'a (dyn Any + Sync + Send),
    vtable: &'static DynIoRuntimeVTable,
}

impl<'a> DynIoRuntime<'a> {
    pub fn from_io_runtime<R>(runtime: &'a R) -> Self
    where
        R: IoRuntime,
    {
        DynIoRuntime {
            runtime: runtime as _,
            vtable: R::VTABLE,
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
