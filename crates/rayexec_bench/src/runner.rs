use std::path::Path;
use std::time::Instant;

use rayexec_error::{RayexecError, Result};
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;

use crate::benchmark::Benchmark;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkTimes {
    pub setup_time_ms: u64,
    pub query_times_ms: Vec<u64>,
}

#[derive(Debug)]
pub struct BenchmarkRunner {
    pub engine: SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>,
    pub benchmark: Benchmark,
}

impl BenchmarkRunner {
    /// Run the benchmark.
    ///
    /// Setup queries will only be ran once while the benchmark queries will be
    /// ran `count` times.
    ///
    /// This will make use of the tokio runtime configured on the engine runtime
    /// for pulling the results.
    pub fn run(&self, count: usize) -> Result<BenchmarkTimes> {
        let handle = self.engine.runtime.tokio_handle().handle()?;

        let setup_start = Instant::now();
        let result: Result<()> = handle.block_on(async {
            for setup_query in &self.benchmark.setup {
                for pending in self.engine.session().query_many(setup_query)? {
                    let _ = pending.execute().await?.collect().await?;
                }
            }
            Ok(())
        });
        let _ = result?;
        let setup_time_ms = Instant::now().duration_since(setup_start).as_millis() as u64;

        let mut query_times_ms = Vec::with_capacity(count);
        for _ in 0..count {
            let bench_start = Instant::now();
            let result: Result<()> = handle.block_on(async {
                for query in &self.benchmark.queries {
                    for pending in self.engine.session().query_many(query)? {
                        let _ = pending.execute().await?.collect().await?;
                    }
                }
                Ok(())
            });
            let _ = result?;
            let query_time_ms = Instant::now().duration_since(bench_start).as_millis() as u64;

            query_times_ms.push(query_time_ms);
        }

        Ok(BenchmarkTimes {
            setup_time_ms,
            query_times_ms,
        })
    }
}
