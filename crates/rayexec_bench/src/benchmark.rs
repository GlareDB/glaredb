use std::path::Path;

use rayexec_error::{RayexecError, Result};
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;

/// Describes a benchmark to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Benchmark {
    /// Setup queries to run prior to running the actual benchmark queries.
    pub setup: Vec<String>,
    /// The benchmark queries.
    pub queries: Vec<String>,
}

impl Benchmark {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }
}
