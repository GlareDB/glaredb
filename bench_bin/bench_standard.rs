use glaredb_bench::EngineBuilder;
use glaredb_error::Result;

pub fn main() -> Result<()> {
    let builder = EngineBuilder::try_new()?;
    glaredb_bench::run(builder, "./bench/standard")?;
    Ok(())
}
