use rayexec_execution::engine::Engine;
use std::path::Path;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/standard")).unwrap();
    rayexec_slt::run(paths, |rt| Engine::new(rt), "slt_standard").unwrap();
}
