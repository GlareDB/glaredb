mod benchmark;
mod runner;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use benchmark::Benchmark;
use clap::Parser;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;
use runner::{BenchmarkRunner, BenchmarkTimes};

#[derive(Parser)]
#[clap(name = "rayexec_bin")]
struct Arguments {
    // /// Print the EXPLAIN output for queries prior to running them.
    // #[clap(long, env = "DEBUG_PRINT_EXPLAIN")]
    // print_explain: bool,
    // /// Print the profile data for a query after running it.
    // #[clap(long, env = "DEBUG_PRINT_PROFILE_DATA")]
    // print_profile_data: bool,
    #[clap(long, short, default_value = "5")]
    count: usize,
    /// Pattern to match benchmark files to run.
    #[clap()]
    pattern: Option<String>,
}

pub trait EngineBuilder {
    fn build(&self) -> Result<SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>>;
}

#[derive(Debug)]
pub struct DefaultEngineBuilder {
    executor: ThreadedNativeExecutor,
    runtime: NativeRuntime,
}

impl DefaultEngineBuilder {
    pub fn try_new() -> Result<Self> {
        Ok(DefaultEngineBuilder {
            executor: ThreadedNativeExecutor::try_new()?,
            runtime: NativeRuntime::with_default_tokio()?,
        })
    }
}

impl EngineBuilder for DefaultEngineBuilder {
    fn build(&self) -> Result<SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>> {
        let registry =
            DataSourceRegistry::default().with_datasource("memory", Box::new(MemoryDataSource))?;

        SingleUserEngine::try_new(self.executor.clone(), self.runtime.clone(), registry)
    }
}

pub fn run(builder: impl EngineBuilder, files: impl IntoIterator<Item = PathBuf>) -> Result<()> {
    let args = Arguments::parse();

    // Times keyed by the file names.
    let mut all_times: BTreeMap<String, BenchmarkTimes> = BTreeMap::new(); // BTree for sorted output.

    for path in files {
        let path_str = path
            .to_str()
            .ok_or_else(|| RayexecError::new("File path not valid utf8"))?;

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

        let times = runner.run(args.count)?;

        let name = path_str
            .trim_end_matches(".bench")
            .trim_start_matches("./")
            .trim_start_matches("../")
            .to_string();
        all_times.insert(name, times);
    }

    // Print results.
    println!(
        "{:<40}\t{:>7}\t{:>14}",
        "benchmark_name", "count", "time_ms"
    );

    for (name, times) in &all_times {
        for (idx, query_time) in times.query_times_ms.iter().enumerate() {
            println!("{:<40}\t{:>7}\t{:>14}", name, idx + 1, query_time);
        }
    }

    // TODO: Allow writing to csv/tsv

    Ok(())
}

/// Recursively find all files in the given directory.
pub fn find_files(dir: &Path) -> Result<Vec<PathBuf>> {
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
