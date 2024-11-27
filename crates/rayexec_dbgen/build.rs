fn main() {
    cc::Build::new()
        // Define DBNAME, LINUX, SQLSERVER, and TPCH.
        //
        // These would have been set in the makefile, but we're not using that.
        // These values don't really matter, just we need them so that
        // everything's configured appropriate (e.g. defining DSS_HUGE for 64
        // bit ints).
        .define("DBNAME", "dss")
        .define("LINUX", None)
        .define("SQLSERVER", None)
        .define("TPCH", None)
        // Included files.
        .files([
            "tpch_dbgen/dbgen.c",
            "tpch_dbgen/permute.c",
            "tpch_dbgen/bm_utils.c",
            "tpch_dbgen/rng64.c",
            "tpch_dbgen/rnd.c",
            "tpch_dbgen/speed_seed.c",
            "tpch_dbgen/varsub.c",
        ])
        .compile("dbgen")
}
