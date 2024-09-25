use rayexec_error::Result;
use rayexec_execution::{
    datasource::{DataSourceBuilder, DataSourceRegistry},
    engine::Engine,
};
use rayexec_parquet::ParquetDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_slt::{ReplacementVars, RunConfig};
use std::{path::Path, sync::Arc, time::Duration};

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new_with_registry(
        ThreadedNativeExecutor::try_new()?,
        rt.clone(),
        DataSourceRegistry::default()
            .with_datasource("parquet", ParquetDataSource::initialize(rt))?,
    )?);

    let paths = rayexec_slt::find_files(Path::new("../slt/tpch")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let session = engine.new_session()?;

            Ok(RunConfig {
                session,
                vars: ReplacementVars::default(),
                create_slt_tmp: false,
                query_timeout: Duration::from_secs(30), // Since these are all running in debug mode.
            })
        },
        "slt_tpch",
    )
}
