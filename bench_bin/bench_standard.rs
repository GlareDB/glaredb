use glaredb_error::Result;
use rayexec_bench::DefaultEngineBuilder;

pub fn main() -> Result<()> {
    let builder = DefaultEngineBuilder::try_new()?;
    rayexec_bench::run(builder, "./bench/standard")?;
    Ok(())
}
