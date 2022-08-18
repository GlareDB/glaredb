//! Execute a query string against an in-memory data source.
use lemur::execute::stream::source::MemoryDataSource;
use sqlengine::engine::Engine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logutil::init(1);

    let query = std::env::args().nth(1).ok_or("expected query argument")?;

    let source = MemoryDataSource::new();
    let mut engine = Engine::new(source);
    engine.ensure_system_tables().await?;
    let mut session = engine.begin_session()?;

    let results = session.execute_query(&query).await?;
    for result in results.into_iter() {
        println!("{:#?}", result);
    }

    Ok(())
}
