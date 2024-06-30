use std::sync::Arc;

use futures::StreamExt;
use rayexec_bullet::format::ugly::ugly_print;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_execution::engine::{Engine, EngineRuntime};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Simple binary for quickly running arbitrary queries.
fn main() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::ERROR.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_test_writer() // TODO: Actually capture
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let runtime = EngineRuntime::try_new_shared().unwrap();
    runtime.clone().tokio.block_on(async move {
        if let Err(e) = inner(runtime).await {
            println!("----");
            println!("ERROR");
            println!("{e}");
            std::process::exit(1);
        }
    })
}

async fn inner(runtime: Arc<EngineRuntime>) -> Result<()> {
    let args: Vec<_> = std::env::args().collect();

    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?;
    let engine = Engine::new_with_registry(runtime, registry)?;
    let mut session = engine.new_session()?;

    let query = args[1].clone();

    let outputs = session.simple(&query).await?;

    for mut output in outputs {
        println!("----");
        println!("INPUT: {query}");
        println!("OUTPUT SCHEMA: {:?}", output.output_schema);

        while let Some(result) = output.stream.next().await {
            let batch = result?;
            let out = ugly_print(&output.output_schema, &[batch])?;
            println!("{out}");
        }

        let dump = output.handle.query_dump();
        println!("----");
        println!("DUMP");
        println!("{dump}");
    }

    Ok(())
}
