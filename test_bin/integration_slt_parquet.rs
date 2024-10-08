use std::path::Path;
use std::time::Duration;

use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry};
use rayexec_parquet::ParquetDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;
use rayexec_slt::{ReplacementVars, RunConfig, VarValue};

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let executor = ThreadedNativeExecutor::try_new()?;

    let paths = rayexec_slt::find_files(Path::new("../slt/parquet")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let executor = executor.clone();
            let rt = rt.clone();
            async move {
                let aws_key = VarValue::sensitive_from_env("AWS_KEY");
                let aws_secret = VarValue::sensitive_from_env("AWS_SECRET");

                let mut vars = ReplacementVars::default();
                vars.add_var("AWS_KEY", aws_key);
                vars.add_var("AWS_SECRET", aws_secret);

                let engine = SingleUserEngine::try_new(
                    executor.clone(),
                    rt.clone(),
                    DataSourceRegistry::default()
                        .with_datasource("parquet", ParquetDataSource::initialize(rt.clone()))?,
                )?;

                Ok(RunConfig {
                    engine,
                    vars,
                    create_slt_tmp: true,
                    query_timeout: Duration::from_secs(5),
                })
            }
        },
        "slt_datasource_parquet",
    )
}
