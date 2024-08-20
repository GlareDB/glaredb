use rayexec_error::Result;
use rayexec_execution::engine::Engine;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_slt::{ReplacementVars, RunConfig};
use std::{path::Path, sync::Arc};

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new(ThreadedNativeExecutor::try_new()?, rt.clone())?);

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
        "slt_standard",
    )
}