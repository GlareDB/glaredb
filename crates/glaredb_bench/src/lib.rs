pub mod pagecache;

mod benchmark;
mod runner;

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;

use benchmark::Benchmark;
use clap::Parser;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::{Result, ResultExt};
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};
use harness::Arguments;
use harness::trial::{Measurement, Trial};
use parking_lot::Mutex;
use runner::{BenchmarkRunner, BenchmarkTimes};
use tokio::runtime::Runtime as TokioRuntime;

#[derive(Debug, Parser, Clone, Copy)]
pub struct BenchArguments {
    /// Number of times to run each benchmark query.
    #[clap(long, default_value_t = 3)]
    pub count: usize,
    /// Print the EXPLAIN output of a benchmark query before running it.
    #[clap(long)]
    pub print_explain: bool,
    /// Print out the profile data for benchmark queries.
    #[clap(long)]
    pub print_profile_data: bool,
    /// Print out the results of each benchmark query.
    #[clap(long)]
    pub print_results: bool,
    /// Drop the fs cache before running the setup or benchmark query.
    #[clap(long)]
    pub drop_cache: bool,
}

#[derive(Debug)]
pub struct RunConfig<E, R>
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    pub session: SingleUserEngine<E, R>,
    pub tokio_rt: TokioRuntime,
}

// TODO: Clean this up. The lock + clone is because libtest mimic has very
// strict requirements on the runner function.
#[derive(Debug, Clone)]
pub struct TsvWriter {
    // TODO: Sue me
    file: Arc<Mutex<Option<BufWriter<File>>>>,
}

impl TsvWriter {
    pub fn try_new(save: Option<PathBuf>) -> Result<Self> {
        let file = match save {
            Some(path) => {
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(path)
                    .context("Failed to open file for write")?;
                Some(BufWriter::new(file))
            }
            None => None,
        };

        Ok(TsvWriter {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn write_header(&self) -> Result<()> {
        let mut file = self.file.lock();
        if let Some(file) = file.as_mut() {
            writeln!(file, "bench_name\tcount\tduration_micros")?;
        }

        Ok(())
    }

    pub fn write(&self, bench_name: String, times: &BenchmarkTimes) -> Result<()> {
        let mut file = self.file.lock();
        if let Some(file) = file.as_mut() {
            for (idx, query_time) in times.query_times.iter().enumerate() {
                writeln!(
                    file,
                    "{}\t{}\t{}",
                    bench_name,
                    idx + 1,
                    query_time.as_micros()
                )?;
            }
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let mut file = self.file.lock();
        if let Some(file) = file.as_mut() {
            file.flush()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunArgs {
    pub print_explain: bool,
    pub print_profile_data: bool,
    pub print_results: bool,
    pub count: usize,
}

/// Runs all provide benchmark paths.
///
/// `conf_fn` is ran for each benchmark, and should return a fresh
/// session/single user engine to use for the benchmark.
///
/// `tag` is used to group the benchmarks.
pub fn run<F>(
    writer: TsvWriter,
    args: Arguments<BenchArguments>,
    paths: impl IntoIterator<Item = PathBuf>,
    conf_fn: F,
    tag: &str,
) -> Result<()>
where
    F: Fn() -> Result<RunConfig<ThreadedNativeExecutor, NativeSystemRuntime>>
        + Sync
        + Send
        + Clone
        + 'static,
{
    let benches: Vec<Trial> = paths
        .into_iter()
        .map(|path| {
            let path_str = path.to_string_lossy();

            let bench_name = path_str
                .as_ref()
                .trim_start_matches("./")
                .trim_start_matches("../")
                .to_string();

            let engine_fn = conf_fn.clone();
            let writer = writer.clone();

            Trial::bench(bench_name.clone(), move |_test_mode| {
                let bench = Benchmark::from_file(path)?;

                if args.extra.drop_cache {
                    pagecache::drop_page_cache()?;
                }

                let conf = engine_fn()?;

                let runner = BenchmarkRunner {
                    engine: conf.session,
                    benchmark: bench,
                };

                let times = conf.tokio_rt.block_on(runner.run(args.extra))?;
                writer.write(bench_name, &times)?;

                Ok(Some(Measurement {
                    avg: times.query_avg(),
                    min: times.query_min(),
                    max: times.query_max(),
                }))
            })
            .with_kind(tag)
        })
        .collect();

    harness::run(&args, benches).exit_if_failed();

    Ok(())
}
