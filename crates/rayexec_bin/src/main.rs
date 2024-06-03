use futures::StreamExt;
use rayexec_bullet::format::ugly::ugly_print;
use rayexec_error::Result;
use rayexec_execution::engine::Engine;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Simple binary for quickly running arbitrary queries.
#[tokio::main(flavor = "current_thread")]
async fn main() {
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
    tracing::subscriber::set_global_default(subscriber).unwrap();

    if let Err(e) = inner().await {
        println!("----");
        println!("ERROR");
        println!("{e}");
        std::process::exit(1);
    }
}

async fn inner() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();

    let engine = Engine::try_new()?;
    let mut session = engine.new_session()?;

    let query = args[1].clone();

    let outputs = session.simple(&query)?;

    for output in outputs {
        let results = output.stream.collect::<Vec<_>>().await;
        let batches = results.into_iter().collect::<Result<Vec<_>>>()?;

        println!("----");
        println!("INPUT: {query}");
        println!("OUTPUT SCHEMA: {:?}", output.output_schema);

        for batch in batches.into_iter() {
            let out = ugly_print(&output.output_schema, &[batch])?;
            println!("{out}");
        }
    }

    Ok(())
}
