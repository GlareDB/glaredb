pub mod pagecache;

mod benchmark;
mod runner;

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use benchmark::Benchmark;
use clap::Parser;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::{Result, ResultExt};
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};
use harness::Arguments;
use harness::trial::{Measurement, Trial};
use runner::BenchmarkRunner;
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
    args: &Arguments<BenchArguments>,
    paths: impl IntoIterator<Item = PathBuf>,
    conf_fn: F,
    tag: &str,
    results_tsv_path: &Path,
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

                Ok(Some(Measurement {
                    durations: times.query_times,
                }))
            })
            .with_kind(tag)
        })
        .collect();

    // TSV results file gets initialized on first measurement to avoid
    // truncating an existing file even if all benchmarks are filtered out.
    let mut tsv_file: Option<BufWriter<File>> = None;

    harness::run(args, benches, |info, measurement| {
        let file = match tsv_file.as_mut() {
            Some(file) => file,
            None => {
                // Open up file, truncate, and write header.
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(results_tsv_path)
                    .context("Failed to open file for write")?;
                let mut file = BufWriter::new(file);

                // Header
                writeln!(file, "bench_name\tcount\tduration_micros")?;
                tsv_file = Some(file);

                tsv_file.as_mut().unwrap()
            }
        };

        for (idx, query_time) in measurement.durations.into_iter().enumerate() {
            writeln!(
                file,
                "{}\t{}\t{}",
                info.name,
                idx + 1,
                query_time.as_micros()
            )?;
        }

        Ok(())
    })
    .exit_if_failed();

    if let Some(mut file) = tsv_file {
        file.flush()?;
    }

    Ok(())
}
