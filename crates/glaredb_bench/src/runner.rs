use std::time::Instant;

use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};

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
    pub engine: SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>,
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
    pub async fn run(&self, conf: RunnerConfig) -> Result<BenchmarkTimes> {
        self.run_inner(conf).await
    }

    async fn run_inner(&self, conf: RunnerConfig) -> Result<BenchmarkTimes> {
        let setup_start = Instant::now();
        for setup_query in &self.benchmark.setup {
            for pending in self.engine.session().query_many(setup_query)? {
                let mut q_res = pending.execute().await?;
                let _ = q_res.output.collect().await?;
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
                        Ok(mut q_res) => match q_res.output.collect().await {
                            Ok(batches) => {
                                println!(
                                    "{}",
                                    PrettyTable::try_new(
                                        &q_res.output_schema,
                                        &batches,
                                        100,
                                        None
                                    )?
                                );
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

                    // TODO: Profile data.

                    let mut q_res = pending.execute().await?;
                    let batches = q_res.output.collect().await?;

                    query_time_ms += Instant::now().duration_since(start).as_millis();

                    if conf.print_results {
                        println!(
                            "{}",
                            PrettyTable::try_new(&q_res.output_schema, &batches, 100, None)?
                        );
                    }

                    // if conf.print_profile_data {
                    //     println!("PLANNING PROFILE DATA");
                    //     match results.planning_profile_data() {
                    //         Some(data) => println!("{data}"),
                    //         None => println!("missing planning profile data"),
                    //     }
                    //     println!("EXECUTION PROFILE DATA");
                    //     match results.execution_profile_data() {
                    //         Some(data) => println!("{data}"),
                    //         None => println!("missing execution profile data"),
                    //     }
                    // }
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
