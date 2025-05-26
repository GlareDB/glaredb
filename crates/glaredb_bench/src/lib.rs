pub mod pagecache;

mod benchmark;
mod runner;

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use benchmark::Benchmark;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::{DbError, Result, ResultExt};
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};
use libtest_mimic::{Arguments, Measurement, Trial};
use runner::{BenchmarkRunner, BenchmarkTimes};
use tokio::runtime::Runtime as TokioRuntime;

#[derive(Debug)]
pub struct RunConfig<E, R>
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    pub session: SingleUserEngine<E, R>,
    pub tokio_rt: TokioRuntime,
}

#[derive(Debug)]
pub struct TsvWriter {
    file: Option<BufWriter<File>>,
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

        Ok(TsvWriter { file })
    }

    pub fn write_header(&mut self) -> Result<()> {
        println!("benchmark_identifier\tcount\ttime_millis");
        if let Some(file) = self.file.as_mut() {
            writeln!(file, "benchmark_identifier\tcount\ttime_millis")?;
        }

        Ok(())
    }

    pub fn write(&mut self, bench_identifier: &str, times: BenchmarkTimes) -> Result<()> {
        // for (idx, query_time) in times.query_times_ms.iter().enumerate() {
        //     println!("{}\t{}\t{}", bench_identifier, idx + 1, query_time);
        // }

        // if let Some(file) = self.file.as_mut() {
        //     for (idx, query_time) in times.query_times_ms.iter().enumerate() {
        //         writeln!(file, "{}\t{}\t{}", bench_identifier, idx + 1, query_time)?;
        //     }
        // }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
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
/// `engine_fn` is ran for each benchmark, and should return a fresh
/// session/single user engine to use for the benchmark.
pub fn run<F>(
    writer: &mut TsvWriter,
    run_args: RunArgs,
    paths: impl IntoIterator<Item = PathBuf>,
    engine_fn: F,
) -> Result<()>
where
    F: Fn() -> Result<RunConfig<ThreadedNativeExecutor, NativeSystemRuntime>>
        + Sync
        + Send
        + Clone
        + 'static,
{
    println!("HELLO?");

    let mut args = Arguments::from_args();
    // Always run the "tests" with one thread (this thread) sequentially. We
    // spin up thread pools in the engine itself.
    args.test_threads = Some(1);
    args.test = false;
    args.bench = true;

    let benches: Vec<Trial> = paths
        .into_iter()
        .map(|path| {
            let path_str = path.to_string_lossy();

            let bench_name = path_str
                .as_ref()
                .trim_start_matches("./")
                .trim_start_matches("../")
                .to_string();

            let engine_fn = engine_fn.clone();

            Trial::bench(bench_name, move |_test_mode| {
                let bench = Benchmark::from_file(path)?;
                let conf = engine_fn()?;

                let runner = BenchmarkRunner {
                    engine: conf.session,
                    benchmark: bench,
                };

                let times = conf.tokio_rt.block_on(runner.run(run_args))?;

                // TODO
                // writer.write(&path.identifier, times)?;

                Ok(Some(Measurement {
                    avg: times.query_avg(),
                    variance: 0, // TODO
                }))
            })
        })
        .collect();

    libtest_mimic::run(&args, benches).exit_if_failed();

    Ok(())
}

/// Recursively find all benchmark files at the given path.
///
/// If the provided path points to a file, then the returned vec will only
/// contain a single path buf pointing to that file.
pub fn find_files(path: &Path) -> Result<Vec<PathBuf>> {
    fn inner(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
        if dir.is_dir() {
            let readdir = fs::read_dir(dir).context("Failed to read directory")?;
            for entry in readdir {
                let entry = entry.context("Failed to get entry")?;
                let path = entry.path();

                let path_str = path
                    .to_str()
                    .ok_or_else(|| DbError::new("Expected utf8 path"))?;
                // We might have READMEs interspersed. Only read .bench files.
                if path.is_file() && !path_str.ends_with(".bench") {
                    continue;
                }

                if path.is_dir() {
                    inner(&path, paths)?;
                } else {
                    paths.push(path.to_path_buf());
                }
            }
        }
        Ok(())
    }

    // TODO: Error if it doesn't end in '.bench'
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    let mut paths = Vec::new();
    inner(path, &mut paths)?;

    Ok(paths)
}
