use rayexec_error::Result;
use rayexec_execution::datasource::DataSourceBuilder;
use rayexec_execution::datasource::DataSourceRegistry;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;
use rayexec_slt::{ReplacementVars, RunConfig};
use std::path::Path;
use std::time::Duration;

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let executor = ThreadedNativeExecutor::try_new()?;
    let paths = rayexec_slt::find_files(Path::new("../slt/postgres")).unwrap();

    rayexec_slt::run(
        paths,
        move || {
            let executor = executor.clone();
            let rt = rt.clone();

            async move {
                let engine = SingleUserEngine::try_new(
                    executor.clone(),
                    rt.clone(),
                    DataSourceRegistry::default()
                        .with_datasource("postgres", PostgresDataSource::initialize(rt.clone()))?,
                )?;

                Ok(RunConfig {
                    engine,
                    vars: ReplacementVars::default(),
                    create_slt_tmp: false,
                    query_timeout: Duration::from_secs(5),
                })
            }
        },
        "slt_datasource_postgres",
    )
}
