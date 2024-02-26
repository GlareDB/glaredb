//! Bin for running the SLTs.

use std::fs;
use std::path::{Path, PathBuf};

use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_sqltest::run_tests;

/// Path to slts directory relative to this crate's root.
const SLTS_PATH: &'static str = "slts/";

pub fn main() -> Result<()> {
    env_logger::init();

    std::panic::set_hook(Box::new(|info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        println!("Info: {}\n\nBacktrace:{}", info, backtrace);
        std::process::abort();
    }));

    let mut paths = Vec::new();
    find_files(Path::new(SLTS_PATH), &mut paths)?;

    run_tests(paths)
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
