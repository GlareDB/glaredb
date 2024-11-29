use std::path::Path;

use rayexec_bench::DefaultEngineBuilder;
use rayexec_error::Result;

pub fn main() -> Result<()> {
    let builder = DefaultEngineBuilder::try_new()?;
    let paths = rayexec_bench::find_files(Path::new("./bench/standard"))?;
    rayexec_bench::run(builder, paths)?;
    Ok(())
}
