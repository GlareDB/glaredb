use std::time::Instant;

use rayexec_error::Result;
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;

use crate::benchmark::Benchmark;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunnerConfig {
    /// How many times to run each benchmark query.
    pub count: usize,
    pub print_explain: bool,
    pub print_profile_data: bool,
    pub print_results: bool,
}

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
    pub fn run(&self, conf: RunnerConfig) -> Result<BenchmarkTimes> {
        let handle = self.engine.runtime.tokio_handle().handle()?;

        let result: Result<_> = handle.block_on(async move { self.run_inner(conf).await });

        result
    }

    async fn run_inner(&self, conf: RunnerConfig) -> Result<BenchmarkTimes> {
        let setup_start = Instant::now();
        for setup_query in &self.benchmark.setup {
            for pending in self.engine.session().query_many(setup_query)? {
                let _ = pending.execute().await?.collect().await?;
            }
        }

        let setup_time_ms = Instant::now().duration_since(setup_start).as_millis() as u64;

        let mut query_times_ms = Vec::with_capacity(conf.count);

        for idx in 0..conf.count {
            let mut query_time_ms = 0;

            for query in &self.benchmark.queries {
                if idx == 0 && conf.print_results {
                    println!("{query}");
                }

                if idx == 0 && conf.print_explain {
                    let sql = format!("EXPLAIN VERBOSE {query}");
                    match self.engine.session().query(&sql).await {
                        Ok(table) => match table.collect().await {
                            Ok(results) => {
                                println!("{}", results.pretty_table(100, None)?);
                            }
                            Err(_) => {
                                println!("explain not available");
                            }
                        },
                        Err(_) => {
                            println!("explain not available");
                        }
                    }
                }

                let start = Instant::now();
                let pending = self.engine.session().query_many(query)?;
                query_time_ms += Instant::now().duration_since(start).as_millis();

                for pending in pending {
                    let start = Instant::now();
                    let results = if conf.print_profile_data {
                        pending
                            .execute()
                            .await?
                            .collect_with_execution_profile()
                            .await?
                    } else {
                        pending.execute().await?.collect().await?
                    };
                    query_time_ms += Instant::now().duration_since(start).as_millis();

                    if conf.print_results {
                        println!("{}", results.pretty_table(100, None)?)
                    }

                    if conf.print_profile_data {
                        println!("PLANNING PROFILE DATA");
                        match results.planning_profile_data() {
                            Some(data) => println!("{data}"),
                            None => println!("missing planning profile data"),
                        }
                        println!("EXECUTION PROFILE DATA");
                        match results.execution_profile_data() {
                            Some(data) => println!("{data}"),
                            None => println!("missing execution profile data"),
                        }
                    }
                }
            }

            query_times_ms.push(query_time_ms as u64);
        }

        Ok(BenchmarkTimes {
            setup_time_ms,
            query_times_ms,
        })
    }
}
