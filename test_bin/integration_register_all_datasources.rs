use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    datasource::{DataSourceRegistry, MemoryDataSource},
    engine::Engine,
    runtime::ExecutionRuntime,
};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::ThreadedExecutionRuntime;
use std::sync::Arc;

fn main() -> Result<()> {
    let runtime = Arc::new(ThreadedExecutionRuntime::try_new()?.with_default_tokio()?);
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?;

    let engine = Engine::new_with_registry(runtime.clone(), registry)?;
    let mut session = engine.new_session()?;

    let tokio_handle = runtime.tokio_handle().unwrap();
    tokio_handle.block_on(async move {
        session.simple("select 1").await?;
        Ok::<(), RayexecError>(())
    })?;

    Ok(())
}
