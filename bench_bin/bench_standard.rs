use glaredb_bench::DefaultEngineBuilder;
use glaredb_error::Result;

pub fn main() -> Result<()> {
    let builder = DefaultEngineBuilder::try_new()?;
    glaredb_bench::run(builder, "./bench/standard")?;
    Ok(())
}
