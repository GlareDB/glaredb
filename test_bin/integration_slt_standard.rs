use std::path::Path;
use std::time::Duration;

use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use glaredb_slt::{ReplacementVars, RunConfig};

pub fn main() -> Result<()> {
    run_multi_threaded()?;
    run_single_threaded()?;
    run_default_threaded()?;
    Ok(())
}

fn run_with_executor(executor: ThreadedNativeExecutor, tag: &str) -> Result<()> {
    let tokio_rt = new_tokio_runtime_for_io()?;
    let rt = NativeSystemRuntime::new(tokio_rt.handle().clone());

    let paths = glaredb_slt::find_files(Path::new("../slt/standard")).unwrap();
    glaredb_slt::run(
        paths,
        move || {
            let executor = executor.clone();
            let rt = rt.clone();
            async move {
                let engine = SingleUserEngine::try_new(executor.clone(), rt.clone())?;

                Ok(RunConfig {
                    engine,
                    vars: ReplacementVars::default(),
                    create_slt_tmp: false,
                    query_timeout: Duration::from_secs(5),
                })
            }
        },
        tag,
    )
}

fn run_default_threaded() -> Result<()> {
    run_with_executor(ThreadedNativeExecutor::try_new()?, "slt_standard/default")
}

fn run_single_threaded() -> Result<()> {
    run_with_executor(
        ThreadedNativeExecutor::try_new_with_num_threads(1)?,
        "slt_standard/single",
    )
}

fn run_multi_threaded() -> Result<()> {
    run_with_executor(
        ThreadedNativeExecutor::try_new_with_num_threads(16)?,
        "slt_standard/multi",
    )
}
