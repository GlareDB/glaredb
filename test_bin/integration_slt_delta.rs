use rayexec_delta::DeltaDataSource;
use rayexec_execution::{datasource::DataSourceRegistry, engine::Engine};
use rayexec_slt::RunConfig;
use std::path::Path;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/delta")).unwrap();
    rayexec_slt::run(
        paths,
        |rt| {
            Engine::new_with_registry(
                rt,
                DataSourceRegistry::default()
                    .with_datasource("parquet", Box::new(DeltaDataSource))?,
            )
        },
        RunConfig::default(),
        "slt_datasource_delta",
    )
    .unwrap();
}
