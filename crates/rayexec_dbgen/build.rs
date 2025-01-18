use std::env;
use std::path::PathBuf;

use bindgen::CargoCallbacks;

fn main() {
    cc::Build::new()
        .files([
            "tpch_dbgen/dbgen.c",
            "tpch_dbgen/permute.c",
            "tpch_dbgen/build.c",
            "tpch_dbgen/bm_utils.c",
            "tpch_dbgen/rng64.c",
            "tpch_dbgen/rnd.c",
            "tpch_dbgen/text.c",
            "tpch_dbgen/speed_seed.c",
        ])
        .compile("dbgen");

    let header_path = PathBuf::from("tpch_dbgen/dbgen.h").canonicalize().unwrap();
    let header_str = header_path.to_str().unwrap();

    let bindings = bindgen::Builder::default()
        .header(header_str)
        .allowlist_function("generate_table_data")
        .parse_callbacks(Box::new(CargoCallbacks))
        .generate()
        .unwrap();

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("tpch_dbgen_bindings.rs");
    bindings.write_to_file(out_path).unwrap()
}
