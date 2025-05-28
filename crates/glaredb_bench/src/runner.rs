use std::time::{Duration, Instant};

use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};

use crate::BenchArguments;
use crate::benchmark::Benchmark;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkTimes {
    pub setup_time: Duration,
    pub query_times: Vec<Duration>,
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
    pub async fn run(&self, conf: BenchArguments) -> Result<BenchmarkTimes> {
        self.run_inner(conf).await
    }

    async fn run_inner(&self, conf: BenchArguments) -> Result<BenchmarkTimes> {
        let setup_start = Instant::now();
        for setup_query in &self.benchmark.setup {
            for pending in self.engine.session().query_many(setup_query)? {
                let mut q_res = pending.execute().await?;
                let _ = q_res.output.collect().await?;
            }
        }

        let setup_time = Instant::now().duration_since(setup_start);

        let mut query_times = Vec::with_capacity(conf.count);

        for idx in 0..conf.count {
            let mut query_time = Duration::default();

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
                                        None,
                                        PRETTY_COMPONENTS,
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
                query_time += Instant::now().duration_since(start);

                for pending in pending {
                    let start = Instant::now();

                    // TODO: Profile data.

                    let mut q_res = pending.execute().await?;
                    let batches = q_res.output.collect().await?;

                    query_time += Instant::now().duration_since(start);

                    if conf.print_results {
                        println!(
                            "{}",
                            PrettyTable::try_new(
                                &q_res.output_schema,
                                &batches,
                                100,
                                None,
                                PRETTY_COMPONENTS
                            )?
                        );
                    }
                }
            }

            query_times.push(query_time);
        }

        Ok(BenchmarkTimes {
            setup_time,
            query_times,
        })
    }
}
