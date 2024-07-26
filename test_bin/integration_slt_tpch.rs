use rayexec_execution::{
    datasource::{DataSourceBuilder, DataSourceRegistry},
    engine::Engine,
};
use rayexec_parquet::ParquetDataSource;
use rayexec_slt::RunConfig;
use std::path::Path;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/tpch")).unwrap();
    rayexec_slt::run(
        paths,
        |sched, rt| {
            Engine::new_with_registry(
                sched,
                rt.clone(),
                DataSourceRegistry::default()
                    .with_datasource("parquet", ParquetDataSource::initialize(rt))?,
            )
        },
        RunConfig::default(),
        "slt_tpch",
    )
    .unwrap();
}
