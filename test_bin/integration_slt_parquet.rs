use rayexec_error::Result;
use rayexec_execution::{
    datasource::{DataSourceBuilder, DataSourceRegistry},
    engine::Engine,
};
use rayexec_parquet::ParquetDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_slt::{ReplacementVars, RunConfig, VarValue};
use std::{path::Path, sync::Arc};

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new_with_registry(
        ThreadedNativeExecutor::try_new()?,
        rt.clone(),
        DataSourceRegistry::default()
            .with_datasource("parquet", ParquetDataSource::initialize(rt))?,
    )?);

    let paths = rayexec_slt::find_files(Path::new("../slt/parquet")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let aws_key = VarValue::sensitive_from_env("AWS_KEY");
            let aws_secret = VarValue::sensitive_from_env("AWS_SECRET");

            let mut vars = ReplacementVars::default();
            vars.add_var("AWS_KEY", aws_key);
            vars.add_var("AWS_SECRET", aws_secret);

            let session = engine.new_session()?;

            Ok(RunConfig {
                session,
                vars,
                create_slt_tmp: true,
            })
        },
        "slt_datasource_parquet",
    )
}
