use futures::{Stream, StreamExt};
use rayexec_execution::engine::{session::Session, Engine};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Simple binary for quickly running arbitrary queries.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::TRACE.into())
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
    let _g = tracing::subscriber::set_default(subscriber);

    let args: Vec<_> = std::env::args().collect();

    let engine = Engine::try_new()?;
    let session = engine.new_session()?;

    let query = args[1].clone();

    let output = session.execute(&query)?;
    let batches = output.stream.collect::<Vec<_>>().await;

    println!("----");
    println!("INPUT: {query}");
    println!("OUTPUT SCHEMA: {:?}", output.output_schema);

    for (idx, batch) in batches.into_iter().enumerate() {
        println!("{idx}: BATCH\n{batch:?}");
    }

    Ok(())
}
