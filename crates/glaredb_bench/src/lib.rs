mod benchmark;
mod pagecache;
mod runner;

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use benchmark::Benchmark;
use clap::Parser;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::{DbError, Result, ResultExt};
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};
use runner::{BenchmarkRunner, BenchmarkTimes};
use tokio::runtime::Runtime as TokioRuntime;

// TODO: Move this out.
#[derive(Parser)]
#[clap(name = "glaredb_bench")]
pub struct Arguments {
    /// Print the EXPLAIN output for queries prior to running them.
    ///
    /// Only printed once.
    #[clap(long, env = "DEBUG_PRINT_EXPLAIN")]
    pub print_explain: bool,
    /// Print the profile data for a query after running it.
    ///
    /// Data is printed for every run of the query.
    #[clap(long, env = "DEBUG_PRINT_PROFILE_DATA")]
    pub print_profile_data: bool,
    /// Print the results of the benchmark queries.
    ///
    /// Results are printed for every run of the query.
    #[clap(long, env = "DEBUG_PRINT_RESULTS")]
    pub print_results: bool,
    /// If we should drop the page cache before the setup phase of a benchmark
    /// file.
    ///
    /// On linux, this will write to procfs. On mac, this will use the `purge`
    /// tool. Both methods require running the binary with sudo.
    #[clap(long, default_value = "false")]
    pub drop_page_cache: bool,
    /// Number of times to run benchmark queries.
    #[clap(long, short, default_value = "3")]
    pub count: usize,
    /// Optionally save results as a TSV to the provided file.
    #[clap(long, short)]
    pub save: Option<PathBuf>,
    /// Path pointing to either a single benchmark file, or a directory
    /// containing benchmark files.
    ///
    /// If provided a directory, the directory will be walked recursively to
    /// find all benchmark files to run.
    #[clap()]
    pub path: PathBuf,
}

impl Arguments {
    pub fn parse() -> Self {
        Parser::parse()
    }
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

#[derive(Debug)]
struct BenchmarkPath {
    identifier: String,
    path: PathBuf,
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
        for (idx, query_time) in times.query_times_ms.iter().enumerate() {
            println!("{}\t{}\t{}", bench_identifier, idx + 1, query_time);
        }

        if let Some(file) = self.file.as_mut() {
            for (idx, query_time) in times.query_times_ms.iter().enumerate() {
                writeln!(file, "{}\t{}\t{}", bench_identifier, idx + 1, query_time)?;
            }
        }

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
    pub drop_page_cache: bool,
}

/// Try to run benchmarks at the given paths, filtering out paths that don't
/// match the pattern.
pub fn run<F>(
    writer: &mut TsvWriter,
    args: RunArgs,
    paths: impl IntoIterator<Item = PathBuf>,
    session_fn: F,
) -> Result<()>
where
    F: Fn() -> Result<RunConfig<ThreadedNativeExecutor, NativeSystemRuntime>>,
{
    let paths: Vec<BenchmarkPath> = paths
        .into_iter()
        .filter_map(|path| {
            let path_str = path.to_str().expect("valid utf8 paths");

            // Make the identifier a bit nicer.
            let identifier = path_str
                .trim_start_matches("./")
                .trim_start_matches("../")
                .to_string();

            Some(BenchmarkPath { identifier, path })
        })
        .collect();

    if paths.is_empty() {
        // Nothing to do.
        return Ok(());
    }

    for path in paths {
        let bench = Benchmark::from_file(&path.identifier, path.path)?;
        let conf = session_fn()?;

        let runner = BenchmarkRunner {
            engine: conf.session,
            benchmark: bench,
        };

        let times = conf.tokio_rt.block_on(runner.run(args))?;

        writer.write(&path.identifier, times)?;
    }

    Ok(())
}

/// Recursively find all files at the given path.
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

    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    let mut paths = Vec::new();
    inner(path, &mut paths)?;

    Ok(paths)
}
