use rayexec_execution::{datasource::DataSourceRegistry, engine::Engine};
use rayexec_postgres::PostgresDataSource;
use std::path::Path;

pub fn main() {
    let paths = rayexec_sqltest::find_files(Path::new("slts/")).unwrap();
    rayexec_sqltest::run(
        paths,
        |rt| {
            Engine::new_with_registry(
                rt,
                DataSourceRegistry::default()
                    .with_datasource("postgres", Box::new(PostgresDataSource))?,
            )
        },
        "postgres_integration_slt",
    )
    .unwrap();
}
