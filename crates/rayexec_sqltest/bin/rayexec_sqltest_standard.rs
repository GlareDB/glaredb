use rayexec_execution::engine::Engine;
use std::path::Path;

pub fn main() {
    let paths = rayexec_sqltest::find_files(Path::new("slts/")).unwrap();
    rayexec_sqltest::run(paths, |rt| Engine::new(rt), "standard_slt").unwrap();
}
