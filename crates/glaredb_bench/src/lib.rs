mod benchmark;
mod runner;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use benchmark::Benchmark;
use clap::Parser;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::{DbError, Result, ResultExt};
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use runner::{BenchmarkRunner, BenchmarkTimes, RunnerConfig};
use tokio::runtime::Runtime as TokioRuntime;

#[derive(Parser)]
#[clap(name = "rayexec_bin")]
struct Arguments {
    /// Print the EXPLAIN output for queries prior to running them.
    ///
    /// Only printed once.
    #[clap(long, env = "DEBUG_PRINT_EXPLAIN")]
    print_explain: bool,
    /// Print the profile data for a query after running it.
    ///
    /// Data is printed for every run of the query.
    #[clap(long, env = "DEBUG_PRINT_PROFILE_DATA")]
    print_profile_data: bool,
    /// Print the results of the benchmark queries.
    ///
    /// Results are printed for every run of the query.
    #[clap(long, env = "DEBUG_PRINT_RESULTS")]
    print_results: bool,
    /// Directory to search for benchmarks in.
    #[clap(long)]
    benches_dir: Option<String>,
    /// Number of times to run benchmark queries
    #[clap(long, short, default_value = "5")]
    count: usize,
    /// Pattern to match benchmark files to run.
    #[clap()]
    pattern: Option<String>,
}

#[derive(Debug)]
pub struct EngineBuilder {
    tokio_rt: TokioRuntime,
    executor: ThreadedNativeExecutor,
    runtime: NativeSystemRuntime,
}

impl EngineBuilder {
    pub fn try_new() -> Result<Self> {
        let tokio_rt = new_tokio_runtime_for_io()?;
        let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

        Ok(EngineBuilder {
            tokio_rt,
            executor: ThreadedNativeExecutor::try_new()?,
            runtime,
        })
    }

    fn build(&self) -> Result<SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>> {
        SingleUserEngine::try_new(self.executor.clone(), self.runtime.clone())
    }
}

pub fn run(builder: EngineBuilder, default_dir: &str) -> Result<()> {
    let args = Arguments::parse();

    let dir = match &args.benches_dir {
        Some(dir) => Path::new(dir),
        None => Path::new(default_dir),
    };
    let paths = find_files(Path::new(dir))?;

    // Times keyed by the file names.
    let mut all_times: BTreeMap<String, BenchmarkTimes> = BTreeMap::new(); // BTree for sorted output.

    for path in paths {
        let path_str = path
            .to_str()
            .ok_or_else(|| DbError::new("File path not valid utf8"))?;

        if let Some(pattern) = &args.pattern {
            if !path_str.contains(pattern) {
                continue;
            }
        }

        let bench = Benchmark::from_file(&path)?;
        let runner = BenchmarkRunner {
            engine: builder.build()?,
            benchmark: bench,
        };

        let times = builder.tokio_rt.block_on(runner.run(RunnerConfig {
            count: args.count,
            print_explain: args.print_explain,
            print_results: args.print_results,
            print_profile_data: args.print_profile_data,
        }))?;

        let name = path_str
            .trim_end_matches(".bench")
            .trim_start_matches("./")
            .trim_start_matches("../")
            .to_string();
        all_times.insert(name, times);
    }

    // Print results.
    println!(
        "{:<60}\t{:>6}\t{:>14}",
        "benchmark_name", "count", "time_millis"
    );

    for (name, times) in &all_times {
        for (idx, query_time) in times.query_times_ms.iter().enumerate() {
            println!("{:<60}\t{:>6}\t{:>14}", name, idx + 1, query_time);
        }
    }

    // TODO: Allow writing to csv/tsv

    Ok(())
}

/// Recursively find all files in the given directory.
fn find_files(dir: &Path) -> Result<Vec<PathBuf>> {
    fn inner(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir).context("read dir")? {
                let entry = entry.context("entry")?;
                let path = entry.path();
                if path.is_dir() {
                    inner(&path, paths)?;
                } else {
                    paths.push(path.to_path_buf());
                }
            }
        }
        Ok(())
    }

    let mut paths = Vec::new();
    inner(dir, &mut paths)?;

    Ok(paths)
}
