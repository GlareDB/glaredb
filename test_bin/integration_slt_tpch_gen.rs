use std::path::Path;
use std::time::Duration;

use ext_tpch_gen::TpchGenExtension;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use glaredb_slt::{ReplacementVars, RunConfig};

pub fn main() -> Result<()> {
    let tokio_rt = new_tokio_runtime_for_io()?;
    let rt = NativeSystemRuntime::new(tokio_rt.handle().clone());
    let executor = ThreadedNativeExecutor::try_new()?;

    let paths = glaredb_slt::find_files(Path::new("../slt/tpch_gen")).unwrap();
    glaredb_slt::run(
        paths,
        move || {
            let executor = executor.clone();
            let rt = rt.clone();
            async move {
                let engine = SingleUserEngine::try_new(executor.clone(), rt.clone())?;
                engine.register_extension(TpchGenExtension)?;

                Ok(RunConfig {
                    engine,
                    vars: ReplacementVars::default(),
                    create_slt_tmp: false,
                    query_timeout: Duration::from_secs(5),
                })
            }
        },
        "slt_tpch_gen",
    )
}
