use std::path::PathBuf;

const GEN_MODULE_DIR: &str = "src/proto/";

fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                "proto/arrow.proto",
                "proto/catalog.proto",
                "proto/service.proto",
                "proto/storage.proto",
                "proto/options.proto",
            ],
            &["proto"],
        )
        .unwrap();

    // Copy generated files to the `proto` modules.
    let src_dest = [
        ("metastore.arrow.rs", "arrow.rs"),
        ("metastore.catalog.rs", "catalog.rs"),
        ("metastore.service.rs", "service.rs"),
        ("metastore.storage.rs", "storage.rs"),
        ("metastore.options.rs", "options.rs"),
    ];
    let out: PathBuf = std::env::var("OUT_DIR").unwrap().into();
    for (src, dest) in src_dest {
        let src = out.clone().join(src);
        let dest = PathBuf::from(GEN_MODULE_DIR).join(dest);
        std::fs::copy(src, dest).unwrap();
    }
}
