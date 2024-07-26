use rayexec_delta::DeltaDataSource;
use rayexec_execution::{
    datasource::{DataSourceBuilder, DataSourceRegistry},
    engine::Engine,
};
use rayexec_slt::{ReplacementVars, RunConfig, VarValue};
use std::path::Path;

pub fn main() {
    let aws_key = VarValue::sensitive_from_env("AWS_KEY");
    let aws_secret = VarValue::sensitive_from_env("AWS_SECRET");

    let mut vars = ReplacementVars::default();
    vars.add_var("AWS_KEY", aws_key);
    vars.add_var("AWS_SECRET", aws_secret);

    let paths = rayexec_slt::find_files(Path::new("../slt/delta")).unwrap();
    rayexec_slt::run(
        paths,
        |sched, rt| {
            Engine::new_with_registry(
                sched,
                rt.clone(),
                DataSourceRegistry::default()
                    .with_datasource("delta", DeltaDataSource::initialize(rt))?,
            )
        },
        RunConfig {
            vars,
            create_slt_tmp: false,
        },
        "slt_datasource_delta",
    )
    .unwrap();
}
