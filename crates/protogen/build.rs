fn main() {
    let mut config = prost_build::Config::new();
    config.btree_map(&[".metastore.options.StorageOptions"]);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(
            config,
            &[
                // Common
                "proto/common/arrow.proto",
                // Metastore
                "proto/metastore/catalog.proto",
                "proto/metastore/service.proto",
                "proto/metastore/storage.proto",
                "proto/metastore/options.proto",
                // rpcsrv
                "proto/rpcsrv/service.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
