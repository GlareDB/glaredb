//! Bin for running the SLTs.

use libtest_mimic::{Arguments, Trial};
use rayexec_error::{Result, ResultExt};
use rayexec_sqltest::run_test;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Builder;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Path to slts directory relative to this crate's root.
const SLTS_PATH: &str = "slts/";

pub fn main() {
    let args = Arguments::from_args();

    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::ERROR.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_test_writer() // TODO: Actually capture
        .with_env_filter(env_filter)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    std::panic::set_hook(Box::new(|info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        println!("---- PANIC ----\nInfo: {}\n\nBacktrace:{}", info, backtrace);
        std::process::abort();
    }));

    let mut paths = Vec::new();
    find_files(Path::new(SLTS_PATH), &mut paths).unwrap();

    let rt = Arc::new(
        Builder::new_current_thread()
            .enable_all()
            .thread_name("rayexec_sqltest")
            .build()
            .unwrap(),
    );

    let tests = paths
        .into_iter()
        .map(|path| {
            let test_name = path.to_string_lossy().to_string();
            let rt = rt.clone();
            Trial::test(test_name, move || match rt.block_on(run_test(path)) {
                Ok(_) => Ok(()),
                Err(e) => Err(e.into()),
            })
        })
        .collect();

    libtest_mimic::run(&args, tests).exit();
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
