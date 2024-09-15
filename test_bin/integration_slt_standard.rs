use rayexec_error::Result;
use rayexec_execution::engine::Engine;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_slt::{ReplacementVars, RunConfig};
use std::{path::Path, sync::Arc};

pub fn main() -> Result<()> {
    run_multi_threaded()?;
    run_single_threaded()?;
    run_default_threaded()?;
    Ok(())
}

fn run_with_engine(
    engine: Arc<Engine<ThreadedNativeExecutor, NativeRuntime>>,
    tag: &str,
) -> Result<()> {
    let paths = rayexec_slt::find_files(Path::new("../slt/standard")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let session = engine.new_session()?;

            Ok(RunConfig {
                session,
                vars: ReplacementVars::default(),
                create_slt_tmp: false,
            })
        },
        tag,
    )
}

fn run_default_threaded() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new(ThreadedNativeExecutor::try_new()?, rt.clone())?);
    run_with_engine(engine, "slt_standard/default")
}

fn run_single_threaded() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new(
        ThreadedNativeExecutor::try_new_with_num_threads(1)?,
        rt.clone(),
    )?);
    run_with_engine(engine, "slt_standard/single")
}

fn run_multi_threaded() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new(
        ThreadedNativeExecutor::try_new_with_num_threads(16)?,
        rt.clone(),
    )?);
    run_with_engine(engine, "slt_standard/multi")
}
