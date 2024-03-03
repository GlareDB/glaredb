//! Bin for running the SLTs.

use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_sqltest::run_tests;
use std::fs;
use std::path::{Path, PathBuf};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Path to slts directory relative to this crate's root.
const SLTS_PATH: &'static str = "slts/";

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::TRACE.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_test_writer() // TODO: Actually capture
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    let _g = tracing::subscriber::set_default(subscriber);

    std::panic::set_hook(Box::new(|info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        println!("---- PANIC ----\nInfo: {}\n\nBacktrace:{}", info, backtrace);
        std::process::abort();
    }));

    let mut paths = Vec::new();
    find_files(Path::new(SLTS_PATH), &mut paths).unwrap();

    if let Err(e) = run_tests(paths).await {
        println!("---- FAIL ----");
        println!("{e}");
        std::process::exit(1);
    }
}

fn find_files(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir).context("read dir")? {
            let entry = entry.context("entry")?;
            let path = entry.path();
            if path.is_dir() {
                find_files(&path, paths)?;
            } else {
                paths.push(path.to_path_buf());
            }
        }
    }
    Ok(())
}
