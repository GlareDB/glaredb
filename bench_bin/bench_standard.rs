use rayexec_bench::DefaultEngineBuilder;
use rayexec_error::Result;

pub fn main() -> Result<()> {
    let builder = DefaultEngineBuilder::try_new()?;
    rayexec_bench::run(builder, "./bench/standard")?;
    Ok(())
}
