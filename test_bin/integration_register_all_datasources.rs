use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    datasource::{DataSourceRegistry, MemoryDataSource},
    engine::{Engine, EngineRuntime},
};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;

fn main() -> Result<()> {
    let runtime = EngineRuntime::try_new_shared().unwrap();
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?;

    let engine = Engine::new_with_registry(runtime.clone(), registry)?;
    let mut session = engine.new_session()?;

    runtime.tokio.block_on(async move {
        session.simple("select 1").await?;
        Ok::<(), RayexecError>(())
    })?;

    Ok(())
}
