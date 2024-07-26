use rayexec_csv::CsvDataSource;
use rayexec_execution::{
    datasource::{DataSourceBuilder, DataSourceRegistry},
    engine::Engine,
};
use rayexec_slt::{ReplacementVars, RunConfig};
use std::path::Path;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/csv")).unwrap();
    rayexec_slt::run(
        paths,
        |sched, rt| {
            Engine::new_with_registry(
                sched,
                rt.clone(),
                DataSourceRegistry::default()
                    .with_datasource("csv", CsvDataSource::initialize(rt))?,
            )
        },
        RunConfig {
            vars: ReplacementVars::default(),
            create_slt_tmp: true,
        },
        "slt_datasource_csv",
    )
    .unwrap();
}
