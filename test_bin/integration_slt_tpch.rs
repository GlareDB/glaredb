use rayexec_execution::{datasource::DataSourceRegistry, engine::Engine};
use rayexec_parquet::ParquetDataSource;
use rayexec_slt::RunConfig;
use std::path::Path;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/tpch")).unwrap();
    rayexec_slt::run(
        paths,
        |rt| {
            Engine::new_with_registry(
                rt,
                DataSourceRegistry::default()
                    .with_datasource("parquet", Box::new(ParquetDataSource))?,
            )
        },
        RunConfig::default(),
        "slt_tpch",
    )
    .unwrap();
}
