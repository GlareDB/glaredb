use rayexec_error::Result;
use rayexec_execution::datasource::DataSourceBuilder;
use rayexec_execution::{datasource::DataSourceRegistry, engine::Engine};
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_slt::{ReplacementVars, RunConfig};
use std::path::Path;
use std::sync::Arc;

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new_with_registry(
        ThreadedNativeExecutor::try_new()?,
        rt.clone(),
        DataSourceRegistry::default()
            .with_datasource("postgres", PostgresDataSource::initialize(rt))?,
    )?);

    let paths = rayexec_slt::find_files(Path::new("../slt/postgres")).unwrap();
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
        "slt_datasource_postgres",
    )
}
